use std::sync::Arc;

use getset::Getters;
use parking_lot::RwLock;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{self, Duration};
use tracing::{error, info, instrument};

use crate::{AppResult, BrokerConfig, Connection, Shutdown};
use crate::config::DynamicConfig;
use crate::request::{ProduceRequestV0, RequestContext, RequestEnum, RequestProcessor};

#[derive(Getters)]
#[get = "pub"]
pub struct Broker {
    broker_config: Arc<BrokerConfig>,
    dynamic_config: RwLock<DynamicConfig>,
}

#[derive(Debug)]
struct Server<'dc> {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    dynamic_config: &'dc RwLock<DynamicConfig>,
    broker_config: Arc<BrokerConfig>,
}

#[derive(Debug)]
struct ConnectionHandler {
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
    broker_config: Arc<BrokerConfig>,
    socket_read_ch_tx: Sender<()>,
    socket_read_ch_rx: Receiver<()>,
}

impl Broker {
    pub fn new(broker_config: Arc<BrokerConfig>) -> Broker {
        let broker_config_clone = Arc::clone(&broker_config);
        Broker {
            broker_config,
            //创建动态配置，默认从静态配置加载，运行时可以动态修改
            //动态配置在启动broker、启动server、客户端新建连接时更新
            //为了减少快照生成时间，相关的动态配置对象尽可能小一些
            dynamic_config: RwLock::new(DynamicConfig::new(broker_config_clone)),
        }
    }

    pub async fn start(&self) {
        let network_conf = self.broker_config.network();
        let listen_address = format!("{}:{}", network_conf.ip(), network_conf.port());
        let dynamic_config_snapshot = {
            let lock = self.dynamic_config.read();
            (*lock).clone()
        };
        let bind_result = TcpListener::bind(&listen_address).await;
        if let Err(err) = &bind_result {
            error!(
                "Failed to bind server to address: {} - Error: {}",
                listen_address, err
            );
            return;
        }
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);
        let mut server = Server {
            listener: bind_result.unwrap(),
            limit_connections: Arc::new(Semaphore::new(dynamic_config_snapshot.max_package_size())),
            notify_shutdown,
            shutdown_complete_tx,
            dynamic_config: &self.dynamic_config,
            broker_config: Arc::clone(&self.broker_config),
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
                let lock = self.dynamic_config.read();
                lock.clone()
            };

            let broker_config_clone = Arc::clone(&self.broker_config);
            //ensure request are processed FIFO in one connection, even client using piping
            let (socket_read_ch_tx, socket_read_ch_rx) = mpsc::channel(1);

            let mut handler = ConnectionHandler {
                connection: Connection::new(socket, dynamic_config_snap_shot),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                broker_config: broker_config_clone,
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
            let frame = match maybe_frame {
                Some(frame) => frame,
                // client close the connection
                None => return Ok(()),
            };

            let request_context = RequestContext::new(&self.connection, &self.broker_config);
            match RequestEnum::try_from(frame) {
                Ok(request) => {
                    RequestProcessor::process_request(request, &request_context);
                }
                Err(error) => {
                    RequestProcessor::respond_invalid_request(error, &request_context);
                }
            }
            //proceed socket read for next request
            self.socket_read_ch_tx.send(()).await?;
        }
        Ok(())
    }
}
