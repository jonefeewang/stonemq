use crossbeam_utils::atomic::AtomicCell;
use dashmap::DashMap;
use futures_util::StreamExt;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::sleep;
use tokio::time::Duration;
use tokio_util::time::{delay_queue, DelayQueue};

use crate::Shutdown;

// 异步的DelayedOperation trait
pub trait DelayedOperation: Send + Sync {
    fn delay_ms(&self) -> u64;

    async fn try_complete(&self) -> bool;

    fn on_complete(&self) -> impl Future<Output = ()> + Send;
    fn on_expiration(&self) -> impl Future<Output = ()> + Send;
}

// 包装DelayedOperation的状态管理
#[derive(Debug)]
pub struct DelayedOperationState<T: DelayedOperation> {
    operation: Arc<T>,
    completed: AtomicCell<bool>,
    delay_key: AtomicCell<Option<delay_queue::Key>>,
}

impl<T: DelayedOperation> DelayedOperationState<T> {
    pub fn new(operation: T) -> Self {
        Self {
            operation: Arc::new(operation),
            completed: AtomicCell::new(false),
            delay_key: AtomicCell::new(None),
        }
    }

    pub fn is_completed(&self) -> bool {
        self.completed.load()
    }

    pub async fn force_complete(&self) -> bool {
        if !self.completed.swap(true) {
            self.operation.on_complete().await;
            true
        } else {
            false
        }
    }
}

// 定义 DelayQueue 的操作枚举
pub(crate) enum DelayQueueOp<T: DelayedOperation> {
    Insert(Arc<DelayedOperationState<T>>, Duration),
    Remove(tokio_util::time::delay_queue::Key),
}

// 管理延迟操作的Purgatory
#[derive(Debug)]
pub struct DelayedOperationPurgatory<T: DelayedOperation + 'static> {
    name: String,
    watchers: DashMap<String, Vec<Arc<DelayedOperationState<T>>>>,
    delay_queue_tx: Sender<DelayQueueOp<T>>,
}

impl<T: DelayedOperation> DelayedOperationPurgatory<T> {
    pub fn new(
        name: &str,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: Sender<()>,
    ) -> (Self, Receiver<DelayQueueOp<T>>, Shutdown) {
        let shutdown = Shutdown::new(notify_shutdown.subscribe());
        let (tx, rx): (Sender<DelayQueueOp<T>>, Receiver<DelayQueueOp<T>>) = mpsc::channel(1000); // 适当的缓冲大小

        let purgatory = DelayedOperationPurgatory {
            name: name.to_string(),
            watchers: DashMap::new(),
            delay_queue_tx: tx,
        };
        (purgatory, rx, shutdown)
    }

    pub async fn try_complete_else_watch(&self, operation: T, watch_keys: Vec<String>) -> bool {
        let op_state = Arc::new(DelayedOperationState::new(operation));

        if op_state.operation.try_complete().await && op_state.force_complete().await {
            return true;
        }

        for key in watch_keys {
            if op_state.is_completed() {
                break;
            }

            self.watchers
                .entry(key)
                .or_insert_with(Vec::new)
                .push(Arc::clone(&op_state));
        }

        if !op_state.is_completed() {
            let delay = Duration::from_millis(op_state.operation.delay_ms());
            self.delay_queue_tx
                .send(DelayQueueOp::Insert(Arc::clone(&op_state), delay))
                .await
                .expect("delay queue sender should be alive");
        }

        false
    }

    pub async fn start(
        self: Arc<Self>,
        mut delay_queue_rx: Receiver<DelayQueueOp<T>>,
        mut shutdown: Shutdown,
    ) {
        // DelayQueue 处理循环
        tokio::spawn(async move {
            let mut delay_queue = DelayQueue::new();

            loop {
                tokio::select! {
                    Some(op) = delay_queue_rx.recv() => {
                        match op {
                            DelayQueueOp::Insert(state, duration) => {
                                let key = delay_queue.insert(state.clone(), duration);
                                state.delay_key.store(Some(key));
                            }
                            DelayQueueOp::Remove(key) => {
                                delay_queue.remove(&key);
                            }
                        }
                    }
                    Some(expired) = delay_queue.next() => {
                        let op = expired.into_inner();
                        if op.force_complete().await {
                            op.operation.on_expiration().await;
                        }
                    }
                    _ = shutdown.recv() => break,
                }
            }
        });

        // 清理循环保持不变
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(60)).await;
                self_clone.purge_completed().await;
            }
        });
    }

    pub async fn check_and_complete(&self, key: &str) -> usize {
        if let Some(watcher_list) = self.watchers.get(key) {
            let mut completed = 0;
            for op in watcher_list.value() {
                if !op.is_completed()
                    && op.operation.try_complete().await
                    && op.force_complete().await
                {
                    completed += 1;
                    if let Some(delay_key) = op.delay_key.load() {
                        self.delay_queue_tx
                            .send(DelayQueueOp::Remove(delay_key))
                            .await
                            .expect("delay queue sender should be alive");
                    }
                }
            }
            completed
        } else {
            0
        }
    }

    async fn purge_completed(&self) {
        let mut keys_to_remove = Vec::new();

        // 遍历entry时按每个entry锁定
        for mut entry in self.watchers.iter_mut() {
            let mut new_ops = Vec::new();

            for op in entry.value() {
                if !op.is_completed() {
                    new_ops.push(Arc::clone(op));
                }
            }

            *entry.value_mut() = new_ops;

            if entry.value().is_empty() {
                keys_to_remove.push(entry.key().clone());
            }
        }

        for key in keys_to_remove {
            self.watchers.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MyAsyncOp;

    // 实现也不需要 async_trait 宏
    impl DelayedOperation for MyAsyncOp {
        fn delay_ms(&self) -> u64 {
            3000
        }

        async fn try_complete(&self) -> bool {
            sleep(Duration::from_millis(100)).await;
            println!("try_complete");
            false
        }

        async fn on_complete(&self) {
            println!("on_complete");
            sleep(Duration::from_millis(50)).await;
        }

        async fn on_expiration(&self) {
            println!("on_expiration");
            sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn test_delayed_operation_purgatory() {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

        let (purgatory, rx, shutdown) =
            DelayedOperationPurgatory::new("test", notify_shutdown, shutdown_complete_tx);
        let purgatory = Arc::new(purgatory);

        // 启动后台任务
        purgatory.clone().start(rx, shutdown).await;

        // 添加延迟操作
        let op = MyAsyncOp;
        purgatory
            .try_complete_else_watch(op, vec!["key1".to_string()])
            .await;

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
