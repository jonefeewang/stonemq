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
use tracing::error;

use crate::message::GroupCoordinator;
use crate::network::{Connection, RequestFrame};
use crate::request::{ApiRequest, RequestProcessor};
use crate::AppResult;
use crate::{AppError, ReplicaManager};

use super::Shutdown;

// 用于生成唯一的连接ID
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
    num_workers: usize,
) -> async_channel::Sender<RequestTask> {
    let (request_tx, request_rx) = async_channel::bounded(1024);
    tokio::spawn(async move {
        let mut workers = HashMap::with_capacity(num_workers);

        // CAUTION: There is a potential risk here: handling client-initiated requests might lead to intentional or unintentional panics,
        // causing widespread processor exits. Although recovery mechanisms can be implemented,
        // such incidents may still result in fluctuations in request processing.

        // 处理客户端上来的请求时，特别是解析协议时，需要做防护，防止客户端故意造成panic
        for i in 0..num_workers {
            let rx: async_channel::Receiver<RequestTask> = request_rx.clone();
            let replica_manager = replica_manager.clone();
            let group_coordinator = group_coordinator.clone();
            let handle = tokio::spawn(async move {
                while let Ok(request) = rx.recv().await {
                    process_request(request, replica_manager.clone(), group_coordinator.clone())
                        .await;
                }
            });
            workers.insert(i, handle);
        }

        // 启动监控线程监控worker状态
        loop {
            // 遍历任务并检查状态
            for id in 0..workers.len() {
                if let Some(handle) = workers.remove(&id) {
                    // 提取出 JoinHandle 并检查其运行状态
                    match time::timeout(Duration::from_millis(200), handle).await {
                        Ok(join_result) => {
                            match join_result {
                                Ok(_) => {
                                    // ignore, task is completed
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
                                            // 打印动态类型的名称
                                            error!(
                                                "Processor Task panicked with an unknown type: {}",
                                                get_type_name(&payload)
                                            );
                                        }
                                        // 重新生成一个新的任务
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

            tokio::time::sleep(Duration::from_secs(2)).await;
        }
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
    let api_request = ApiRequest::try_from((request_body, &request_header));
    if let Ok(api_request) = api_request {
        // 处理请求的具体逻辑
        let response = RequestProcessor::process_request(
            api_request,
            client_ip,
            &request_header,
            replica_manager,
            group_coordinator,
        )
        .await;
        if let Err(e) = response_tx.send(response) {
            // 这种发送错误，往往是无法恢复的，直接打印错误，继续处理下一个请求，无需反馈给客户端
            error!("Failed to send response: {:?}", e);
        }
    } else {
        // 解析请求失败，返回错误响应,发送给客户端
        error!(
            "Failed to parse request: {:?} for connection: {}",
            &request_header, connection_id
        );
        todo!("返回错误响应给客户端")
    }
}

// 每个连接的处理器
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
            // 读取请求 如果客户端优雅关闭连接，则返回None，如果意外关闭，则返回Err
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = shutdown.recv() => {
                    return Ok(());
                }
            };

            let frame = match maybe_frame {
                Some(frame) => frame,
                // client close the connection gracefully
                None => break,
            };

            // 为每个请求创建一个新的 oneshot channel
            let (response_tx, response_rx) = oneshot::channel();

            // 发送到全局处理队列
            let request = RequestTask {
                connection_id: self.connection_id,
                client_ip: self.connection.client_ip.clone(),
                frame,
                response_tx,
            };

            if let Err(e) = self.request_tx.send(request).await {
                error!("Failed to send request: {:?}", e);
                return Err(AppError::ConnectionError("Failed to send request".into()));
            }

            // 等待响应并写入
            match response_rx.await {
                Ok(response) => {
                    self.writer.write_all(&response).await?;
                    self.writer.flush().await?;
                }
                Err(_) => {
                    // 请求处理器panic意外退出，没有发送响应，关闭连接
                    error!("Request processor dropped without sending response");
                    return Err(AppError::TaskError("Response channel closed".into()));
                }
            }
        }

        Ok(())
    }
}

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
    /// Graceful shutdown sequence:  
    /// 1. The run loop terminates upon receiving the shutdown signal from the upper layer.  
    /// 2. The connection handler shuts down, but only after all requests on the connection have been fully processed and the responses sent.  
    /// 3. Once all connection handlers have exited, the receiver in the request handler receives the shutdown signal and exits by returning an error.  
    /// 4. The main process waits for the connection handler to drop, during which its `shutdown_complete_tx` field is also automatically dropped, before gracefully exiting.
    ///
    /// # Returns
    /// Under normal operations, continuously accept new connections.  
    /// Exit with an error if failing to accept new connections.
    ///
    /// Returns `AppResult<()>` indicating the success or failure of running the server.
    pub async fn run(&self) -> AppResult<()> {
        let request_sender = start_request_handler(
            self.replica_manager.clone(),
            self.group_coordinator.clone(),
            num_cpus::get(),
        );

        loop {
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let socket = self.accept().await?;
            let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
            let (reader, writer) = socket.into_split();

            let notify_shutdown_clone = self.notify_shutdown.clone();
            let shutdown_complete_tx = self.shutdown_complete_tx.clone();

            let mut handler = ConnectionHandler {
                _shutdown_complete_tx: shutdown_complete_tx,
                notify_shutdown: notify_shutdown_clone,
                connection_id,
                connection: Connection::new(reader),
                writer: BufWriter::new(writer),
                request_tx: request_sender.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.handle_connection().await {
                    error!("Connection error: {:?}", err);
                }
                // 不管是优雅关闭，还是意外关闭，都要释放连接
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
                        return Err(err.into());
                    }
                }
            }

            time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
        }
    }
}
