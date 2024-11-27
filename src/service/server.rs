use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::message::GroupCoordinator;
use crate::network::{Connection, RequestFrame};
use crate::protocol::ApiKey;
use crate::request::{self, ApiRequest, RequestContext, RequestProcessor};
use crate::{AppError, DynamicConfig};
use crate::{AppResult, ReplicaManager, Shutdown};

struct ConnectionHandler {
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
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
            let (reader, writer) = socket.into_split();

            let handler = ConnectionHandler {
                connection: Connection::new(reader, dynamic_config_snapshot),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                replica_manager: self.replica_manager.clone(),
                group_coordinator: self.group_coordinator.clone(),
            };
            tokio::spawn(async move {
                if let Err(err) = handler.run(writer).await {
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
    async fn run(self, writer_half: OwnedWriteHalf) -> AppResult<()> {
        // 创建 mpsc 通道用于消息传递
        let (common_request_tx, mut common_request_rx) =
            mpsc::channel::<(BytesMut, RequestContext)>(32);
        let (common_response_tx, mut common_response_rx) = mpsc::channel::<Bytes>(32);
        let (produce_request_tx, mut produce_request_rx) =
            mpsc::channel::<(BytesMut, RequestContext)>(32);
        let (produce_response_tx, mut produce_response_rx) = mpsc::channel::<Bytes>(32);

        // 在 spawn 之前，将需要的数据从 self 中移出
        let mut connection = self.connection;
        let mut shutdown = self.shutdown;
        let replica_manager = self.replica_manager.clone();
        let group_coordinator = self.group_coordinator.clone();

        // 任务：读取数据
        let reader_task = tokio::spawn(async move {
            while !shutdown.is_shutdown() {
                /* 要么读取一个frame，处理请求，要么收到关闭消息，从handle返回，结束当前connection的处理 */
                let maybe_frame = tokio::select! {
                    res = {
                        connection.read_frame()
                    } => res,
                    _ = shutdown.recv() => {
                        return Ok(());
                    }
                };
                if maybe_frame.is_err() {
                    return Err(maybe_frame.err().unwrap());
                }
                let frame = match maybe_frame.unwrap() {
                    Some(frame) => frame,
                    // client close the connection
                    None => return Ok(()),
                };
                let RequestFrame {
                    request_body,
                    request_header,
                } = frame;

                let api_key = request_header.api_key;

                let request_context = RequestContext::new(
                    request_header,
                    replica_manager.clone(),
                    group_coordinator.clone(),
                );

                if api_key == ApiKey::Produce {
                    if let Err(e) = produce_request_tx
                        .send((request_body, request_context))
                        .await
                    {
                        error!(
                            "Failed to send produce request data to processing task: {:?}",
                            e
                        );
                        return Err(AppError::SendChannelError(e.to_string()));
                    }
                } else if let Err(e) = common_request_tx
                    .send((request_body, request_context))
                    .await
                {
                    error!(
                        "Failed to send common request data to processing task: {:?}",
                        e
                    );
                    return Err(AppError::SendChannelError(e.to_string()));
                }
            }
            // 收到shutdown信号了，正常退出
            Ok(())
        });

        // 任务：业务逻辑处理
        let processor_task = tokio::spawn(async move {
            let mut handles = Vec::new();

            while let Some((request_body, mut request_context)) = common_request_rx.recv().await {
                let response_tx = common_response_tx.clone();

                let handle = tokio::spawn(async move {
                    let response =
                        match ApiRequest::try_from((request_body, &request_context.request_header))
                        {
                            Ok(request) => {
                                RequestProcessor::process_request(request, &mut request_context)
                                    .await
                            }
                            Err(error) => {
                                warn!("Invalid request: {:?}", error);
                                RequestProcessor::respond_invalid_request(error, &request_context)
                                    .await
                            }
                        };

                    if let Err(e) = response_tx.send(response).await {
                        error!(
                            "Failed to send common response data to writing task: {:?}",
                            e
                        );
                        return Err(AppError::SendChannelError(e.to_string()));
                    }
                    Ok(())
                });

                handles.push(handle);

                if handles.len() >= 100 {
                    let (completed, _, remaining) = futures::future::select_all(handles).await;
                    handles = remaining;
                }
            }

            for handle in handles {
                match handle.await {
                    Ok(result) => match result {
                        Ok(()) => (),
                        Err(e) => return Err(e),
                    },
                    Err(e) => return Err(AppError::TaskError(e.to_string())),
                }
            }
            Ok(())
        });

        let produce_processor_task = tokio::spawn(async move {
            let mut handles = Vec::new();

            while let Some((request_body, mut request_context)) = produce_request_rx.recv().await {
                let response_tx = produce_response_tx.clone();

                let handle = tokio::spawn(async move {
                    let response =
                        match ApiRequest::try_from((request_body, &request_context.request_header))
                        {
                            Ok(request) => {
                                RequestProcessor::process_request(request, &mut request_context)
                                    .await
                            }
                            Err(error) => {
                                warn!("Invalid request: {:?}", error);
                                RequestProcessor::respond_invalid_request(error, &request_context)
                                    .await
                            }
                        };

                    if let Err(e) = response_tx.send(response).await {
                        error!(
                            "Failed to send produce response data to writing task: {:?}",
                            e
                        );
                        return Err(AppError::SendChannelError(e.to_string()));
                    }
                    Ok(())
                });

                handles.push(handle);

                if handles.len() >= 100 {
                    let (completed, _, remaining) = futures::future::select_all(handles).await;
                    handles = remaining;
                }
            }

            for handle in handles {
                match handle.await {
                    Ok(result) => match result {
                        Ok(()) => (),
                        Err(e) => return Err(e),
                    },
                    Err(e) => return Err(AppError::TaskError(e.to_string())),
                }
            }
            Ok(())
        });

        // 任务：写数据
        let writer_task = tokio::spawn(async move {
            let mut writer = BufWriter::new(writer_half);
            while let Some(response) = tokio::select! {
                response = produce_response_rx.recv() => response,
                response = common_response_rx.recv() => response,
            } {
                if let Err(e) = writer.write_all(&response).await {
                    error!("Failed to write response: {:?}", e);
                    return Err(AppError::NetworkWriteError(e.to_string().into()));
                }
            }
            Ok(())
        });

        // 等待所有任务完成
        match tokio::try_join!(
            Self::flatten(reader_task),
            Self::flatten(produce_processor_task),
            Self::flatten(processor_task),
            Self::flatten(writer_task)
        ) {
            Ok(((), (), (), ())) => {
                info!("connection closed gracefully");
                Ok(())
            }
            Err(err) => {
                error!("connection closed with error: {:?}", err);
                Err(err)
            }
        }
    }
    async fn flatten<T>(handle: JoinHandle<Result<T, AppError>>) -> Result<T, AppError> {
        match handle.await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(AppError::CommonError(err.to_string())),
        }
    }
}
