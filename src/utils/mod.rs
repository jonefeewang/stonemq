mod delayed_async_operation;
mod mini_kv_db;
mod multiple_channel_worker_pool;
mod worker_pool;

use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
pub use multiple_channel_worker_pool::MultipleChannelWorkerPool;
pub use multiple_channel_worker_pool::PoolHandler;
pub use multiple_channel_worker_pool::WorkerPoolConfig;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::time::delay_queue;

use tokio::sync::mpsc::Sender;

// 管理延迟操作的Purgatory
#[derive(Debug)]
pub struct DelayedAsyncOperationPurgatory<T: DelayedAsyncOperation + 'static> {
    name: String,
    watchers: DashMap<String, Vec<Arc<DelayedAsyncOperationState<T>>>>,
    delay_queue_tx: Sender<DelayQueueOp<T>>,
    _shutdown_complete_tx: Sender<()>,
}

// 异步的DelayedOperation trait
pub trait DelayedAsyncOperation: Send + Sync {
    fn delay_ms(&self) -> u64;
    fn try_complete(&self) -> impl Future<Output = bool> + Send;
    fn on_complete(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
    fn on_expiration(&self) -> impl Future<Output = ()> + Send;
}

// 包装DelayedOperation的状态管理
#[derive(Debug)]
struct DelayedAsyncOperationState<T: DelayedAsyncOperation> {
    operation: Arc<T>,
    completed: AtomicCell<bool>,
    delay_key: AtomicCell<Option<delay_queue::Key>>,
    is_expired: AtomicCell<bool>,
}

// 定义 DelayQueue 的操作枚举
enum DelayQueueOp<T: DelayedAsyncOperation> {
    Insert(Arc<DelayedAsyncOperationState<T>>, Duration),
    Remove(tokio_util::time::delay_queue::Key),
}
