use std::borrow::Cow;
use std::sync::Arc;

use getset::Getters;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore};
use tokio::time::{self, Duration};
use tracing::{error, info, instrument, trace};

use crate::{
    AppResult, BROKER_CONFIG, BrokerConfig, Connection, LogManager, ReplicaManager, Shutdown,
};
use crate::AppError::IllegalStateError;
use crate::config::DynamicConfig;
use crate::request::{ApiRequest, RequestContext, RequestProcessor};

#[derive(Getters)]
#[get = "pub"]
pub struct Broker {
    dynamic_config: Arc<RwLock<DynamicConfig>>,
    replica_manager: Arc<ReplicaManager>,
}

struct Server {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    replica_manager: Arc<ReplicaManager>,
    dynamic_config: Arc<RwLock<DynamicConfig>>,
}

struct ConnectionHandler {
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
    socket_read_ch_tx: mpsc::Sender<()>,
    socket_read_ch_rx: mpsc::Receiver<()>,
    replica_manager: Arc<ReplicaManager>,
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
            dynamic_config: Arc::new(RwLock::new(DynamicConfig::new())),
            replica_manager: Arc::new(replica_manager),
        }
    }
    pub async fn start(&mut self) -> AppResult<()> {
        // 开启logManager和replicaManager的定时线程
        let log_manager = self.replica_manager.log_manager.clone();
        log_manager.start_task().await?;

        let network_conf = &BROKER_CONFIG.get().unwrap().network;
        let listen_address = format!("{}:{}", network_conf.ip, network_conf.port);

        let bind_result = TcpListener::bind(&listen_address).await;
        if let Err(err) = &bind_result {
            let error_msg = format!(
                "Failed to bind server to address: {} - Error: {}",
                listen_address, err
            );
            error!(error_msg);
            return Err(IllegalStateError(Cow::Owned(error_msg)));
        }
        info!("Binding to {} for listening", &listen_address);
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
        let max_connection = {
            let lock = self.dynamic_config.read().await;
            lock.max_connection()
        };
        let mut server = Server {
            listener: bind_result.unwrap(),
            limit_connections: Arc::new(Semaphore::new(max_connection)),
            notify_shutdown,
            shutdown_complete_tx,
            replica_manager: self.replica_manager.clone(),
            dynamic_config: self.dynamic_config.clone(),
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

impl Server {
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
            let frame = match maybe_frame {
                Some(frame) => frame,
                // client close the connection
                None => return Ok(()),
            };

            let mut request_context = RequestContext::new(
                &mut self.connection,
                frame.request_header,
                self.replica_manager.clone(),
            );
            trace!("Received request: {:?}", &request_context.request_header);
            match ApiRequest::try_from((frame.body, &request_context.request_header)) {
                Ok(request) => {
                    RequestProcessor::process_request(request, &mut request_context).await?
                }
                Err(error) => {
                    RequestProcessor::respond_invalid_request(error, &request_context).await?
                }
            };
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
            self.socket_read_ch_tx.send(()).await?;
            trace!("proceed socket read for next request")
        }
        Ok(())
    }
}
