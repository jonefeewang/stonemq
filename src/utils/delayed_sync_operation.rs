use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
use futures_util::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::sleep;
use tokio::time::Duration;
use tokio_util::time::{delay_queue, DelayQueue};

use crate::Shutdown;

// 异步的DelayedOperation trait
pub trait DelayedSyncOperation: Send + Sync {
    fn delay_ms(&self) -> u64;
    fn try_complete(&self) -> bool;
    fn on_complete(&self);
    fn on_expiration(&self);
}

// 包装DelayedOperation的状态管理
#[derive(Debug)]
struct DelayedOperationState<T: DelayedSyncOperation> {
    operation: Arc<T>,
    completed: AtomicCell<bool>,
    delay_key: AtomicCell<Option<delay_queue::Key>>,
}

impl<T: DelayedSyncOperation> DelayedOperationState<T> {
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

    pub fn force_complete(&self) -> bool {
        if !self.completed.swap(true) {
            self.operation.on_complete();
            true
        } else {
            false
        }
    }
}

// 定义 DelayQueue 的操作枚举
enum DelayQueueOp<T: DelayedSyncOperation> {
    Insert(Arc<DelayedOperationState<T>>, Duration),
    Remove(tokio_util::time::delay_queue::Key),
}

// 管理延迟操作的Purgatory
#[derive(Debug)]
pub struct DelayedSyncOperationPurgatory<T: DelayedSyncOperation + 'static> {
    name: String,
    watchers: DashMap<String, Vec<Arc<DelayedOperationState<T>>>>,
    delay_queue_tx: Sender<DelayQueueOp<T>>,
}

impl<T: DelayedSyncOperation> DelayedSyncOperationPurgatory<T> {
    pub async fn new(
        name: &str,
        notify_shutdown: broadcast::Sender<()>,
        _shutdown_complete_tx: Sender<()>,
    ) -> Arc<Self> {
        let shutdown = Shutdown::new(notify_shutdown.subscribe());
        let (tx, rx): (Sender<DelayQueueOp<T>>, Receiver<DelayQueueOp<T>>) = mpsc::channel(1000); // 适当的缓冲大小

        let purgatory = DelayedSyncOperationPurgatory {
            name: name.to_string(),
            watchers: DashMap::new(),
            delay_queue_tx: tx,
        };
        // (purgatory, rx, shutdown)
        let purgatory = Arc::new(purgatory);
        purgatory.clone().start(rx, shutdown).await;
        purgatory
    }

    pub async fn try_complete_else_watch(&self, operation: T, watch_keys: Vec<String>) -> bool {
        let op_state = Arc::new(DelayedOperationState::new(operation));

        if op_state.operation.try_complete() && op_state.force_complete() {
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

    async fn start(
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
                        if op.force_complete() {
                            op.operation.on_expiration();
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
                if !op.is_completed() && op.operation.try_complete() && op.force_complete() {
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
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    // 测试用的操作结构体
    struct TestOperationState {
        should_complete: AtomicBool,
        complete_count: AtomicU64,
        expire_count: AtomicU64,
    }

    struct TestOperation {
        id: u64,
        delay: u64,
        state: Arc<TestOperationState>,
    }

    impl TestOperation {
        fn new(id: u64, delay: u64) -> (Self, Arc<TestOperationState>) {
            let state = Arc::new(TestOperationState {
                should_complete: AtomicBool::new(false),
                complete_count: AtomicU64::new(0),
                expire_count: AtomicU64::new(0),
            });

            (
                Self {
                    id,
                    delay,
                    state: state.clone(),
                },
                state,
            )
        }
    }

    impl DelayedSyncOperation for TestOperation {
        fn delay_ms(&self) -> u64 {
            self.delay
        }

        fn try_complete(&self) -> bool {
            self.state.should_complete.load(Ordering::Relaxed)
        }

        fn on_complete(&self) {
            self.state.complete_count.fetch_add(1, Ordering::Relaxed);
        }

        fn on_expiration(&self) {
            self.state.expire_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn test_immediate_completion() {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

        let purgatory = DelayedSyncOperationPurgatory::new(
            "test_immediate",
            notify_shutdown.clone(),
            shutdown_complete_tx.clone(),
        )
        .await;
        let (op, state) = TestOperation::new(1, 1000);
        state.should_complete.store(true, Ordering::Relaxed);

        assert!(
            purgatory
                .try_complete_else_watch(op, vec!["key1".to_string()])
                .await
        );
        assert_eq!(state.complete_count.load(Ordering::Relaxed), 1);

        // shutdown gracefully
        notify_shutdown.send(());
        shutdown_complete_rx.recv().await.unwrap();
    }

    #[tokio::test]
    async fn test_delayed_completion() {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

        let purgatory = DelayedSyncOperationPurgatory::new(
            "test_delayed",
            notify_shutdown.clone(),
            shutdown_complete_tx.clone(),
        )
        .await;
        let (op, state) = TestOperation::new(2, 1000);

        assert!(
            !purgatory
                .try_complete_else_watch(op, vec!["key1".to_string()])
                .await
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
        state.should_complete.store(true, Ordering::Relaxed);

        assert_eq!(purgatory.check_and_complete("key1").await, 1);
        assert_eq!(state.complete_count.load(Ordering::Relaxed), 1);
        assert_eq!(state.expire_count.load(Ordering::Relaxed), 0);

        // shutdown gracefully
        notify_shutdown.send(());
        shutdown_complete_rx.recv().await.unwrap();
    }

    #[tokio::test]
    async fn test_expiration() {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

        let purgatory = DelayedSyncOperationPurgatory::new(
            "test_expiration",
            notify_shutdown.clone(),
            shutdown_complete_tx.clone(),
        )
        .await;
        let (op, state) = TestOperation::new(3, 100); // 短延迟以便快速测试

        assert!(
            !purgatory
                .try_complete_else_watch(op, vec!["key1".to_string()])
                .await
        );

        // 等待操作过期
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 给一些时间让过期检查线程运行
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(state.complete_count.load(Ordering::Relaxed), 1);
        assert_eq!(state.expire_count.load(Ordering::Relaxed), 1);
        // shutdown gracefully
        notify_shutdown.send(());
        shutdown_complete_rx.recv().await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_keys() {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

        let purgatory = DelayedSyncOperationPurgatory::new(
            "test_multiple",
            notify_shutdown.clone(),
            shutdown_complete_tx.clone(),
        )
        .await;
        let (op, state) = TestOperation::new(4, 1000);

        assert!(
            !purgatory
                .try_complete_else_watch(op, vec!["key1".to_string(), "key2".to_string()])
                .await
        );

        state.should_complete.store(true, Ordering::Relaxed);

        assert_eq!(purgatory.check_and_complete("key1").await, 1);
        assert_eq!(purgatory.check_and_complete("key2").await, 0); // 已经完成，不会重复完成
        assert_eq!(state.complete_count.load(Ordering::Relaxed), 1);

        // shutdown gracefully
        notify_shutdown.send(());
        shutdown_complete_rx.recv().await.unwrap();
    }

    #[tokio::test]
    async fn test_purge_completed() {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

        let purgatory = DelayedSyncOperationPurgatory::new(
            "test_purge",
            notify_shutdown.clone(),
            shutdown_complete_tx.clone(),
        )
        .await;
        let (op, state) = TestOperation::new(5, 1000);

        assert!(
            !purgatory
                .try_complete_else_watch(op, vec!["key1".to_string()])
                .await
        );

        state.should_complete.store(true, Ordering::Relaxed);
        assert_eq!(purgatory.check_and_complete("key1").await, 1);

        // 等待清理循环运行
        tokio::time::sleep(Duration::from_secs(61)).await;

        // 验证 key1 已被清理
        assert!(!purgatory.watchers.contains_key("key1"));
        // shutdown gracefully
        notify_shutdown.send(());
        shutdown_complete_rx.recv().await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

        let purgatory = DelayedSyncOperationPurgatory::new(
            "test_concurrent",
            notify_shutdown.clone(),
            shutdown_complete_tx.clone(),
        )
        .await;
        let mut states = Vec::new();
        let ops: Vec<_> = (0..10)
            .map(|i| {
                let (op, state) = TestOperation::new(i, 500);
                states.push(state);
                op
            })
            .collect();

        // 并发添加操作
        let handles: Vec<_> = ops
            .into_iter()
            .map(|op| {
                let purgatory = purgatory.clone();
                tokio::spawn(async move {
                    purgatory
                        .try_complete_else_watch(op, vec!["shared_key".to_string()])
                        .await;
                })
            })
            .collect();

        // 等待所有线程完成
        for handle in handles {
            handle.await.unwrap();
        }

        // 验证所有操作都被正确添加
        assert_eq!(purgatory.watchers.get("shared_key").unwrap().len(), 10);

        // 验证可以完成所有操作
        for state in states {
            state.should_complete.store(true, Ordering::Relaxed);
        }
        assert_eq!(purgatory.check_and_complete("shared_key").await, 10);

        // shutdown gracefully
        notify_shutdown.send(());
        shutdown_complete_rx.recv().await.unwrap();
    }
}
