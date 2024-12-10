use crate::group_consume::GroupCoordinator;
use crate::log::LogManager;
use crate::replica::ReplicaManager;
use crate::service::Server;
use crate::AppError::{self, IllegalStateError};
use crate::{global_config, AppResult};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::signal;
use tokio::sync::mpsc::Sender;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tracing::{debug, error, info};

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

pub struct Broker;

impl Broker {
    pub fn start(rt: &Runtime) -> AppResult<()> {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

        // startup log manager
        let log_manager = LogManager::new(notify_shutdown.clone(), shutdown_complete_tx.clone());
        let log_manager = log_manager.startup(rt)?;

        // startup replica manager
        let mut replica_manager = ReplicaManager::new(
            log_manager.clone(),
            notify_shutdown.clone(),
            shutdown_complete_tx.clone(),
            rt,
        );
        replica_manager.startup(rt)?;
        let replica_manager = Arc::new(replica_manager);

        rt.block_on(Self::run_tcp_server(
            replica_manager.clone(),
            notify_shutdown.clone(),
            shutdown_complete_tx.clone(),
        ))?;

        // tcp server has been shutdown, send shutdown signal
        notify_shutdown
            .send(())
            .map_err(|e| AppError::ChannelSendError(e.to_string()))?;
        drop(log_manager);
        drop(replica_manager);
        drop(shutdown_complete_tx);
        debug!("waiting for shutdown complete...");
        rt.block_on(async {
            shutdown_complete_rx.recv().await;
        });

        info!("broker shutdown complete");
        Ok(())
    }

    async fn run_tcp_server(
        replica_manager: Arc<ReplicaManager>,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: Sender<()>,
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
            return Err(IllegalStateError(error_msg.to_string()));
        }
        info!("tcp server binding to {} for listening", &listen_address);
        let max_connection = global_config().network.max_connection;

        let group_config = global_config().group_consume.clone();
        let node = Node::new_localhost();
        let group_coordinator = GroupCoordinator::startup(
            group_config,
            notify_shutdown.clone(),
            shutdown_complete_tx.clone(),
            node,
        )
        .await;
        let group_coordinator = Arc::new(group_coordinator);

        let server = Server::new(
            bind_result
                .map_err(|e| AppError::DetailedIoError(format!("bind tcp server error: {}", e)))?,
            Arc::new(Semaphore::new(max_connection)),
            notify_shutdown.clone(),
            shutdown_complete_tx,
            replica_manager.clone(),
            group_coordinator.clone(),
        );

        info!("server startup complete");
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
