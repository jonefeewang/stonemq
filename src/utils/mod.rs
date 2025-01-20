// Copyright 2025 jonefeewang@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod delayed_async_operation;
mod multiple_channel_worker_pool;
mod single_channel_worker_pool;

pub use multiple_channel_worker_pool::MultipleChannelWorkerPool;
pub use multiple_channel_worker_pool::PoolHandler;
pub use multiple_channel_worker_pool::WorkerPoolConfig;

use dashmap::DashMap;
use parking_lot::RwLock;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
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
    completed: AtomicBool,
    delay_key: RwLock<Option<delay_queue::Key>>,
    is_expired: AtomicBool,
}

// define the operation of delay queue
enum DelayQueueOp<T: DelayedAsyncOperation> {
    Insert(Arc<DelayedAsyncOperationState<T>>, Duration),
    Remove(tokio_util::time::delay_queue::Key),
}
