mod delayed_async_operation;
mod multiple_channel_worker_pool;
mod single_channel_worker_pool;

pub use multiple_channel_worker_pool::MultipleChannelWorkerPool;
pub use multiple_channel_worker_pool::PoolHandler;
pub use multiple_channel_worker_pool::WorkerPoolConfig;

use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio_util::time::delay_queue;

// manage delayed async operation
#[derive(Debug)]
pub struct DelayedAsyncOperationPurgatory<T: DelayedAsyncOperation + 'static> {
    name: String,
    watchers: DashMap<String, Vec<Arc<DelayedAsyncOperationState<T>>>>,
    delay_queue_tx: Sender<DelayQueueOp<T>>,
    _shutdown_complete_tx: Sender<()>,
}

// delayed async operation trait
pub trait DelayedAsyncOperation: Send + Sync {
    fn delay_ms(&self) -> u64;
    fn try_complete(&self) -> impl Future<Output = bool> + Send;
    fn on_complete(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
    fn on_expiration(&self) -> impl Future<Output = ()> + Send;
}

// manage the state of delayed operation
#[derive(Debug)]
struct DelayedAsyncOperationState<T: DelayedAsyncOperation> {
    operation: Arc<T>,
    completed: AtomicCell<bool>,
    delay_key: AtomicCell<Option<delay_queue::Key>>,
    is_expired: AtomicCell<bool>,
}

// define the operation of delay queue
enum DelayQueueOp<T: DelayedAsyncOperation> {
    Insert(Arc<DelayedAsyncOperationState<T>>, Duration),
    Remove(tokio_util::time::delay_queue::Key),
}
