use std::borrow::Cow;
use std::sync::Arc;

use getset::Getters;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{self, Duration};
use tracing::{error, info, instrument};

use crate::{
    AppResult, BROKER_CONFIG, BrokerConfig, Connection, LogManager, ReplicaManager, Shutdown,
};
use crate::AppError::IllegalStateError;
use crate::config::DynamicConfig;
use crate::request::{ApiRequest, RequestContext, RequestProcessor};

#[derive(Getters)]
#[get = "pub"]
pub struct Broker {
    dynamic_config: RwLock<DynamicConfig>,
    replica_manager: ReplicaManager,
}

#[derive(Debug)]
struct Server<'dc> {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    dynamic_config: &'dc RwLock<DynamicConfig>,
}

#[derive(Debug)]
struct ConnectionHandler {
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
    socket_read_ch_tx: Sender<()>,
    socket_read_ch_rx: Receiver<()>,
}

impl Broker {
    pub fn new(replica_manager: ReplicaManager) -> Self {
        Broker {
            //创建动态配置，默认从静态配置加载，运行时可以动态修改
            //动态配置在启动broker、启动server、客户端新建连接时更新
            //为了减少快照生成时间，相关的动态配置对象尽可能小一些
            // Create dynamic configuration, initially loading from static configuration,
            // with runtime modifications allowed
            // Dynamic configuration updates when the broker starts, the server starts,
            // and new client connections are established
            // To minimize snapshot generation time, keep related dynamic configuration objects
            // as small as possible
            dynamic_config: RwLock::new(DynamicConfig::new()),
            replica_manager,
        }
    }
    pub async fn start(&mut self) -> AppResult<()> {
        let network_conf = &BROKER_CONFIG.get().unwrap().network;
        let listen_address = format!("{}:{}", network_conf.ip, network_conf.port);
        let dynamic_config_snapshot = {
            let lock = self.dynamic_config.read().await;
            (*lock).clone()
        };
        let bind_result = TcpListener::bind(&listen_address).await;
        if let Err(err) = &bind_result {
            let error_msg = format!(
                "Failed to bind server to address: {} - Error: {}",
                listen_address, err
            );
            error!(error_msg);
            return Err(IllegalStateError(Cow::Owned(error_msg)));
        }
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
        let mut server = Server {
            listener: bind_result.unwrap(),
            limit_connections: Arc::new(Semaphore::new(dynamic_config_snapshot.max_package_size())),
            notify_shutdown,
            shutdown_complete_tx,
            dynamic_config: &self.dynamic_config,
        };

        tokio::select! {
          res = server.run() => {
              if let Err(err) = res {
                  error!(cause = %err, "failed to accept");
              }
          }
          _ = signal::ctrl_c() => {
              info!("shutting down");
          }
        }

        let Server {
            shutdown_complete_tx,
            notify_shutdown,
            ..
        } = server;
        drop(notify_shutdown);
        drop(shutdown_complete_tx);
        let _ = shutdown_complete_rx.recv().await;
        Ok(())
    }
}

impl<'dc> Server<'dc> {
    async fn run(&mut self) -> AppResult<()> {
        info!("accepting inbound connections");
        loop {
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let socket = self.accept().await?;

            let dynamic_config_snap_shot = {
                let lock = self.dynamic_config.read().await;
                lock.clone()
            };

            //ensure request are processed FIFO in one connection, even client using piping
            let (socket_read_ch_tx, socket_read_ch_rx) = mpsc::channel(1);

            let mut handler = ConnectionHandler {
                connection: Connection::new(socket, dynamic_config_snap_shot),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                socket_read_ch_tx,
                socket_read_ch_rx,
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
    #[instrument(skip(self))]
    async fn run(&mut self) -> AppResult<()> {
        self.socket_read_ch_tx.send(()).await?;
        while !self.shutdown.is_shutdown() {
            /* 要么读取一个frame，处理请求，要么收到关闭消息，从handle返回，结束当前connection的处理 */
            let maybe_frame = tokio::select! {
                res ={
                    self.socket_read_ch_rx.recv().await;
                    self.connection.read_frame()
                }=> res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };
            let mut frame = match maybe_frame {
                Some(frame) => frame,
                // client close the connection
                None => return Ok(()),
            };

            let request_context = RequestContext::new(&mut self.connection, &frame.request_header);
            match ApiRequest::try_from(&frame) {
                Ok(request) => {
                    RequestProcessor::process_request(request, &request_context);
                }
                Err(error) => {
                    RequestProcessor::respond_invalid_request(error, &request_context);
                }
            }
            // 发送响应....
            // ...
            // 这里不需要管理缓冲区BytesMut会处理，只要通过advance移动指针后,
            // 再次写入的数据会由BytesMut重复使用之前的空间
            // Note: kafka的网络读取, 一个connection一次会读取一批request(假设客户端使用piping)，(这样的方式
            // 能否提升性能？) 但是一次只会处理一个，相关的配置由queueMaxBytes和socketRequestMaxBytes控制，
            // StoneMQ只读取一个请求，处理完后再读取下一个请求

            // 清空使用过的缓冲区, 供BytesMut回收再利用
            // 但是这里可能body已经被split掉了很多，最后很可能是空的
            frame.body.clear();

            //
            //发送完响应后，继续读取下一个请求

            //proceed socket read for next request
            self.socket_read_ch_tx.send(()).await?;
        }
        Ok(())
    }
}
