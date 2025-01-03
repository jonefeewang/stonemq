use std::any::type_name;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, oneshot, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info};

use crate::group_consume::GroupCoordinator;
use crate::network::{Connection, RequestFrame};
use crate::replica::ReplicaManager;
use crate::request::{ApiRequest, RequestContext, RequestProcessor};
use crate::AppError;
use crate::AppResult;

use super::config::RequestHandlerPool;
use super::{global_config, Shutdown};

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct RequestTask {
    connection_id: u64,
    client_ip: String,
    frame: RequestFrame,
    response_tx: oneshot::Sender<BytesMut>,
}
fn get_type_name<T>(_: &T) -> &'static str {
    type_name::<T>()
}

fn start_request_handler(
    replica_manager: Arc<ReplicaManager>,
    group_coordinator: Arc<GroupCoordinator>,
    request_handler_config: &RequestHandlerPool,
    notify_shutdown: broadcast::Sender<()>,
) -> async_channel::Sender<RequestTask> {
    let (request_tx, request_rx) = async_channel::bounded(request_handler_config.channel_capacity);
    let num_channels = request_handler_config.num_channels as usize;
    let mut workers = HashMap::with_capacity(num_channels);
    let monitor_interval = request_handler_config.monitor_interval;
    let worker_check_timeout = request_handler_config.worker_check_timeout;
    tokio::spawn(async move {
        // CAUTION: There is a potential risk here: handling client-initiated requests might lead to intentional or unintentional panics,
        // causing widespread processor exits. Although recovery mechanisms can be implemented,
        // such incidents may still result in fluctuations in request processing.
        for i in 0..num_channels {
            let rx: async_channel::Receiver<RequestTask> = request_rx.clone();
            let replica_manager = replica_manager.clone();
            let group_coordinator = group_coordinator.clone();
            let handle = tokio::spawn(async move {
                debug!("request handler worker {} started", i);
                while let Ok(request) = rx.recv().await {
                    process_request(request, replica_manager.clone(), group_coordinator.clone())
                        .await;
                }
                debug!("request handler worker {} exited", i);
            });
            workers.insert(i, handle);
        }

        // start monitor thread to monitor worker status
        let mut shutdown = Shutdown::new(notify_shutdown.subscribe());
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    debug!("request handler monitor received shutdown signal");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_secs(monitor_interval)) => {}
            }
            // iterate over tasks and check status
            for id in 0..workers.len() {
                if let Some(handle) = workers.remove(&id) {
                    // extract JoinHandle and check its running status
                    match time::timeout(Duration::from_millis(worker_check_timeout), handle).await {
                        Ok(join_result) => {
                            match join_result {
                                Ok(_) => {
                                    info!("request handler monitor found worker {} is exited normally", id);
                                }
                                Err(join_error) => {
                                    if join_error.is_panic() {
                                        let payload = join_error.into_panic();
                                        if let Some(message) =
                                            payload.downcast_ref::<&'static str>()
                                        {
                                            error!(
                                                "Processor Task panicked with message: {}",
                                                message
                                            );
                                        } else if let Some(message) =
                                            payload.downcast_ref::<String>()
                                        {
                                            error!(
                                                "Processor Task panicked with message: {}",
                                                message
                                            );
                                        } else {
                                            // print dynamic type name
                                            error!(
                                                "Processor Task panicked with an unknown type: {}",
                                                get_type_name(&payload)
                                            );
                                        }
                                        // re-generate a new task
                                        let rx = request_rx.clone();
                                        let replica_manager = replica_manager.clone();
                                        let group_coordinator = group_coordinator.clone();

                                        let new_worker = tokio::spawn(async move {
                                            while let Ok(request) = rx.recv().await {
                                                process_request(
                                                    request,
                                                    replica_manager.clone(),
                                                    group_coordinator.clone(),
                                                )
                                                .await;
                                            }
                                        });
                                        workers.insert(id, new_worker);
                                    } else {
                                        error!("Processor Task failed for unknown reasons.");
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            // ignore, task is running
                        }
                    }
                }
            }
        }
        debug!("request handler exit monitor loop");
    });
    request_tx
}

async fn process_request(
    request: RequestTask,
    replica_manager: Arc<ReplicaManager>,
    group_coordinator: Arc<GroupCoordinator>,
) {
    let RequestFrame {
        request_body,
        request_header,
    } = request.frame;
    let RequestTask {
        connection_id,
        client_ip,
        response_tx,
        ..
    } = request;
    let (api_request, error_response) = ApiRequest::parse_from((request_body, &request_header));
    if let Some(api_request) = api_request {
        // handle request logic
        let context = RequestContext {
            client_ip,
            request_header,
            replica_manager,
            group_coordinator,
        };
        let response = RequestProcessor::process_request(api_request, &context).await;
        if let Err(e) = response_tx.send(response) {
            // this send error is usually irrecoverable, print error and continue to process next request, no need to feedback to client
            error!("Failed to send response: {:?}", e);
        }
    } else {
        // parse request failed, if it is apiversion request, return a complete response with error code, otherwise drop the oneshot sender and close the connection
        error!(
            "Failed to parse request: {:?} for connection: {}",
            &request_header, connection_id
        );
        if let Some(error_response) = error_response {
            if let Err(e) = response_tx.send(error_response) {
                error!("Failed to send response: {:?}", e);
            }
        } else {
            drop(response_tx);
        }
    }
}

// handler for each connection
struct ConnectionHandler {
    notify_shutdown: broadcast::Sender<()>,
    _shutdown_complete_tx: mpsc::Sender<()>,
    connection_id: u64,
    connection: Connection,
    writer: BufWriter<OwnedWriteHalf>,
    request_tx: async_channel::Sender<RequestTask>,
}

impl ConnectionHandler {
    async fn handle_connection(&mut self) -> AppResult<()> {
        let mut shutdown = Shutdown::new(self.notify_shutdown.subscribe());
        loop {
            // read request from client, if client close the connection gracefully, return None,
            //if client close the connection unexpectedly, return Err
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = shutdown.recv() => {
                    debug!("connection handler exit read loop after recv shutdown signal");
                    return Ok(());
                }
            };

            let frame = match maybe_frame {
                Some(frame) => frame,
                // client close the connection gracefully
                None => break,
            };

            // create a new oneshot channel for each request
            let (response_tx, response_rx) = oneshot::channel();

            // send request to global request queue
            let request = RequestTask {
                connection_id: self.connection_id,
                client_ip: self.connection.client_ip.clone(),
                frame,
                response_tx,
            };

            if let Err(e) = self.request_tx.send(request).await {
                error!("Failed to send request: {:?}", e);
                return Err(AppError::ChannelSendError(e.to_string()));
            }

            // wait for response and write to client
            match response_rx.await {
                Ok(response) => {
                    self.writer.write_all(&response).await.map_err(|e| {
                        AppError::DetailedIoError(format!("write response error: {}", e))
                    })?;
                    self.writer.flush().await.map_err(|e| {
                        AppError::DetailedIoError(format!("flush response error: {}", e))
                    })?;
                }
                Err(_) => {
                    // request processor panic and exit without sending response, close connection
                    error!("Request processor dropped without sending response");
                    return Err(AppError::IllegalStateError(
                        "Response channel closed".into(),
                    ));
                }
            }
        }
        debug!("connection handler exit read loop");

        Ok(())
    }
}

#[derive(Debug)]
pub struct Server {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    replica_manager: Arc<ReplicaManager>,
    group_coordinator: Arc<GroupCoordinator>,
}

impl Server {
    pub fn new(
        listener: TcpListener,
        limit_connections: Arc<Semaphore>,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
        replica_manager: Arc<ReplicaManager>,
        group_coordinator: Arc<GroupCoordinator>,
    ) -> Self {
        Server {
            listener,
            limit_connections,
            notify_shutdown,
            shutdown_complete_tx,
            replica_manager,
            group_coordinator,
        }
    }

    /// Asynchronously runs the server, handling incoming connections and requests.
    ///
    /// This function starts the request handler, accepts incoming connections, and processes them.
    /// It acquires a permit for each connection to limit the number of concurrent connections.
    /// Each connection is assigned a unique connection ID and handled by a separate ConnectionHandler.
    /// Any errors encountered during connection handling are logged.
    ///
    // Graceful shutdown sequence:
    // 1. The `run loop` is canceled upon receiving the shutdown signal from the upper layer.
    // 2. The `connection handler` continues execution until it receives the shutdown signal. At that point, it stops reading new requests.
    //    Before this, all requests on the connection are fully processed, and responses are sent.
    // 3. Once all `connection handlers` exit, the `sender` they hold is fully dropped. This triggers the `receiver` in the `request handler`
    //     to receive the shutdown signal, causing it to return an error and exit the while loop.
    // 4. The `monitor` in the `request handler` will automatically exit upon receiving the shutdown signal.
    // 5. The `main function` waits for the `connection handler` to drop. Once its `shutdown_complete_tx` field is also dropped, the program exits gracefully.
    // 6. The `splitter task`, as a downstream service, must actively and gracefully exit.
    // 7. The `checkpoint task` in the `log manager`, as a downstream service, must also actively and gracefully exit.
    // 8. The `partition appender` and `active log segments writer`, as upstream services, must wait to be closed later by the runtime.
    // todo:
    // 1. Since the `checkpoint` may perform checks before the `splitter` appends logs, the `log manager` must remove any dirty data appended
    //    after the checkpoint during startup.
    // 2. The `DelayedAsyncOperation`, as an upstream service, must be closed by the runtime and cannot be closed actively. Since it holds components
    //    like the `replica manager` and `coordinator`, these components must not hold the `shutdown_complete_tx` sender; otherwise, it will block the program's shutdown.
    ///
    /// # Returns
    /// Under normal operations, continuously accept new connections.  
    /// Exit with an error if failing to accept new connections.
    ///
    /// Returns `AppResult<()>` indicating the success or failure of running the server.
    #[tracing::instrument]
    pub async fn run(&self) -> AppResult<()> {
        let request_handler_config = &global_config().request_handler_pool;

        let request_sender = start_request_handler(
            self.replica_manager.clone(),
            self.group_coordinator.clone(),
            request_handler_config,
            self.notify_shutdown.clone(),
        );
        let buffer_size = global_config().network.conn_read_buffer_size;

        loop {
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            debug!("accept new connection");

            let socket = self.accept().await?;

            let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
            let (reader, writer) = socket.into_split();

            let notify_shutdown_clone = self.notify_shutdown.clone();
            let shutdown_complete_tx = self.shutdown_complete_tx.clone();

            let mut handler = ConnectionHandler {
                _shutdown_complete_tx: shutdown_complete_tx,
                notify_shutdown: notify_shutdown_clone,
                connection_id,
                connection: Connection::new(reader, buffer_size),
                writer: BufWriter::new(writer),
                request_tx: request_sender.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.handle_connection().await {
                    error!("Connection error: {:?}", err);
                }
                // whether gracefully or unexpectedly closed, release connection
                drop(permit);
            });
        }
    }

    async fn accept(&self) -> AppResult<TcpStream> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        return Err(AppError::DetailedIoError(format!(
                            "accept tcp server error: {}",
                            err
                        )));
                    }
                }
            }

            time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        debug!("tcp server dropped");
    }
}
impl Drop for ConnectionHandler {
    fn drop(&mut self) {
        debug!("connection handler dropped");
    }
}
