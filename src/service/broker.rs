use crate::service::Server;
use crate::AppError::IllegalStateError;
use crate::DynamicConfig;
use crate::{global_config, AppResult, LogManager, ReplicaManager};
use std::borrow::Cow;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore};
use tokio::{runtime, signal};
use tracing::{error, info, trace};

#[derive(Clone, Debug)]
pub struct Node {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}
impl Node {
    pub fn new_localhost() -> Self {
        Node {
            node_id: 0,
            host: global_config().network.ip.clone(),
            port: global_config().network.port as i32,
            rack: None,
        }
    }
}

pub struct Broker {
    dynamic_config: Arc<RwLock<DynamicConfig>>,
}

impl Broker {
    pub fn new() -> Self {
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
        }
    }
    pub fn start(&mut self) -> AppResult<()> {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

        // startup tokio runtime
        let rt = runtime::Builder::new_multi_thread().enable_all().build()?;

        // startup log manager
        let log_manager = LogManager::new(notify_shutdown.clone(), shutdown_complete_tx.clone());
        let log_manager = log_manager.startup(&rt)?;

        // startup replica manager
        let mut replica_manager = ReplicaManager::new(log_manager.clone());
        replica_manager.startup(&rt)?;
        let replica_manager = Arc::new(replica_manager);

        let dynamic_config = self.dynamic_config.clone();

        rt.block_on(Self::run_tcp_server(
            replica_manager.clone(),
            dynamic_config,
            notify_shutdown.clone(),
            shutdown_complete_tx,
            &mut shutdown_complete_rx,
        ))?;

        // tcp server has been shutdown, send shutdown signal
        notify_shutdown.send(())?;
        drop(log_manager);
        drop(replica_manager);
        // wait for shutdown complete
        trace!("waiting for shutdown complete...");
        rt.block_on(shutdown_complete_rx.recv());
        info!("broker shutdown complete");
        Ok(())
    }

    async fn run_tcp_server(
        replica_manager: Arc<ReplicaManager>,
        dynamic_config: Arc<RwLock<DynamicConfig>>,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: Sender<()>,
        shutdown_complete_rx: &mut Receiver<()>,
    ) -> AppResult<()> {
        let network_conf = &global_config().network;
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
        info!("tcp server binding to {} for listening", &listen_address);
        let max_connection = {
            let lock = dynamic_config.read().await;
            lock.max_connection()
        };
        let mut server = Server::new(
            bind_result?,
            Arc::new(Semaphore::new(max_connection)),
            notify_shutdown,
            shutdown_complete_tx,
            replica_manager.clone(),
            dynamic_config.clone(),
        );
        tokio::select! {
          res = server.run() => {
              if let Err(err) = res {
                  error!(cause = %err, "failed to accept");
              }
          }
          _ = signal::ctrl_c() => {
              info!("get shutdown signal");
          }
        }

        Ok(())
    }
}
