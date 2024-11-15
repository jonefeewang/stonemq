use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore};
use tokio::time::{self, Duration};
use tracing::{error, info, instrument, trace};

use crate::message::GroupCoordinator;
use crate::network::Connection;
use crate::request::{ApiRequest, RequestContext, RequestProcessor};
use crate::DynamicConfig;
use crate::{AppResult, ReplicaManager, Shutdown};

struct ConnectionHandler {
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
    socket_read_ch_tx: mpsc::Sender<()>,
    socket_read_ch_rx: mpsc::Receiver<()>,
    replica_manager: Arc<ReplicaManager>,
    group_coordinator: Arc<GroupCoordinator>,
}
pub struct Server {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    replica_manager: Arc<ReplicaManager>,
    dynamic_config: Arc<RwLock<DynamicConfig>>,
    group_coordinator: Arc<GroupCoordinator>,
}

impl Server {
    pub fn new(
        listener: TcpListener,
        limit_connections: Arc<Semaphore>,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
        replica_manager: Arc<ReplicaManager>,
        dynamic_config: Arc<RwLock<DynamicConfig>>,
        group_coordinator: Arc<GroupCoordinator>,
    ) -> Self {
        Server {
            listener,
            limit_connections,
            notify_shutdown,
            shutdown_complete_tx,
            replica_manager,
            dynamic_config,
            group_coordinator,
        }
    }

    pub async fn run(&mut self) -> AppResult<()> {
        info!("tcp server accepting inbound connections");
        loop {
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let socket = self.accept().await?;

            let dynamic_config_snapshot = {
                let lock = self.dynamic_config.read().await;
                lock.clone()
            };

            //ensure request are processed FIFO in one connection, even client using piping
            let (socket_read_ch_tx, socket_read_ch_rx) = mpsc::channel(1);

            let mut handler = ConnectionHandler {
                connection: Connection::new(socket, dynamic_config_snapshot),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                socket_read_ch_tx,
                socket_read_ch_rx,
                replica_manager: self.replica_manager.clone(),
                group_coordinator: self.group_coordinator.clone(),
            };
            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    // package oversize or data corrupted, end connection
                    error!(cause = ?err, "connection error");
                }
                drop(permit);
            });
        }
    }

    async fn accept(&mut self) -> AppResult<TcpStream> {
        let mut backoff = 1;
        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }
            time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
        }
    }
}

impl ConnectionHandler {
    async fn run(&mut self) -> AppResult<()> {
        self.socket_read_ch_tx.send(()).await?;
        while !self.shutdown.is_shutdown() {
            /* 要么读取一个frame，处理请求，要么收到关闭消息，从handle返回，结束当前connection的处理 */
            let maybe_frame = tokio::select! {
                res ={
                    self.socket_read_ch_rx.recv().await;
                    trace!("read frame-------------------------------------");
                    self.connection.read_frame()
                }=> res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };
            let frame = match maybe_frame {
                Some(frame) => frame,
                // client close the connection
                None => return Ok(()),
            };

            let mut request_context = RequestContext::new(
                &mut self.connection,
                frame.request_header,
                self.socket_read_ch_tx.clone(),
                self.replica_manager.clone(),
                self.group_coordinator.clone(),
            );
            info!("Received request: {:?}", &request_context.request_header);
            match ApiRequest::try_from((frame.body, &request_context.request_header)) {
                Ok(request) => {
                    RequestProcessor::process_request(request, &mut request_context).await?
                }
                Err(error) => {
                    RequestProcessor::respond_invalid_request(error, &request_context).await?;
                    self.socket_read_ch_tx.send(()).await?;
                }
            };
            info!("Finished processing request");
            // 发送响应....
            // ...
            // 这里不需要管理缓冲区BytesMut会处理，只要通过advance移动指针后,
            // 再次写入的数据会由BytesMut重复使用之前的空间
            // Note: kafka的网络读取, 一个connection一次会读取一批request(假设客户端使用piping)，(这样的方式
            // 能否提升性能？) 但是一次只会处理一个，相关的配置由queueMaxBytes和socketRequestMaxBytes控制，
            // StoneMQ只读取一个请求，处理完后再读取下一个请求

            // 清空使用过的缓冲区, 供BytesMut回收再利用
            // 但是这里可能body已经被split掉了很多，最后很可能是空的
            // frame.body.clear();

            //
            //发送完响应后，继续读取下一个请求

            //proceed socket read for next request
            // 信号下沉至处理函数中，由处理函数来控制，因为有些请求比如join group，需要等待其他成员加入后才能发送响应（由函数自己控制的话，可以提前发送信号）
            // self.socket_read_ch_tx.send(()).await?;
            // trace!("proceed socket read for next request")
        }
        Ok(())
    }
}
