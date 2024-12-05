use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::sleep;
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tokio_util::time::DelayQueue;
use tracing::{debug, trace};

use crate::Shutdown;

use super::{
    DelayQueueOp, DelayedAsyncOperation, DelayedAsyncOperationPurgatory, DelayedAsyncOperationState,
};
impl<T: DelayedAsyncOperation> DelayedAsyncOperationState<T> {
    pub fn new(operation: Arc<T>) -> Self {
        Self {
            operation,
            completed: AtomicCell::new(false),
            delay_key: AtomicCell::new(None),
            is_expired: AtomicCell::new(false),
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

impl<T: DelayedAsyncOperation> DelayedAsyncOperationPurgatory<T> {
    pub async fn new(
        name: &str,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: Sender<()>,
    ) -> Arc<Self> {
        let (tx, rx): (Sender<DelayQueueOp<T>>, Receiver<DelayQueueOp<T>>) = mpsc::channel(1000); // 适当的缓冲大小

        let purgatory = DelayedAsyncOperationPurgatory {
            name: name.to_string(),
            watchers: DashMap::new(),
            delay_queue_tx: tx,
            _shutdown_complete_tx: shutdown_complete_tx,
        };
        // (purgatory, rx, shutdown)
        let purgatory = Arc::new(purgatory);
        purgatory.clone().start(rx, notify_shutdown).await;
        purgatory
    }

    pub async fn try_complete_else_watch(
        &self,
        operation: Arc<T>,
        watch_keys: Vec<String>,
    ) -> bool {
        trace!(
            "try complete else watch delay:{} with key:{}",
            operation.delay_ms(),
            watch_keys.join(",")
        );
        let op_state = Arc::new(DelayedAsyncOperationState::new(operation));

        if op_state.operation.try_complete().await && op_state.force_complete().await {
            return true;
        }

        for key in watch_keys {
            if op_state.is_completed() {
                break;
            }

            self.watchers
                .entry(key)
                .or_default()
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
        notify_shutdown: broadcast::Sender<()>,
    ) {
        // DelayQueue 处理循环
        let purgatory_name = self.name.clone();

        let mut delay_queue_shutdown = Shutdown::new(notify_shutdown.clone().subscribe());
        let mut purge_shutdown = Shutdown::new(notify_shutdown.clone().subscribe());

        tokio::spawn(async move {
            let mut delay_queue = DelayQueue::new();

            loop {
                tokio::select! {
                    Some(op) = delay_queue_rx.recv() => {
                        match op {
                            DelayQueueOp::Insert(state, duration) => {
                                let key = delay_queue.insert(state.clone(), duration);
                                state.delay_key.store(Some(key));
                                trace!(
                                    "purgatory {} insert delay queue {:?}, duration: {}",
                                    &purgatory_name,
                                    key,
                                    duration.as_millis()
                                );
                            }
                            DelayQueueOp::Remove(key) => {
                                delay_queue.remove(&key);
                                trace!(
                                    "purgatory {} remove delay queue {:?}",
                                    &purgatory_name,
                                    key
                                );
                            }
                        }
                    }
                    Some(expired) = delay_queue.next() => {
                        trace!("purgatory {} delay got expired", &purgatory_name);
                        let op = expired.into_inner();
                        if op.force_complete().await {
                            op.operation.on_expiration().await;
                            op.is_expired.store(true);
                            trace!(
                                "purgatory {} operation expired",
                                &purgatory_name
                            );
                        }
                    }
                    _ = delay_queue_shutdown.recv() => break,
                }
            }
        });

        // 清理循环保持不变
        let self_clone = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = purge_shutdown.recv() => break,
                    _ = async {
                        sleep(Duration::from_secs(60)).await;
                        self_clone.purge_completed().await;
                    } => {}
                }
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
                        trace!("check and complete remove delay queue key: {:?}", delay_key);
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
impl<T: DelayedAsyncOperation> Drop for DelayedAsyncOperationPurgatory<T> {
    fn drop(&mut self) {
        debug!("purgatory {} dropped", self.name);
    }
}

#[cfg(test)]
mod tests {
    use rstest::{fixture, rstest};

    use crate::service::setup_local_tracing;

    use super::*;
    use std::{future::Future, pin::Pin, time::Duration};

    #[fixture]
    #[once]
    fn setup() {
        setup_local_tracing().expect("failed to setup tracing");
    }

    struct TestShortDelayedOperation {
        should_complete: AtomicCell<bool>,
        completed: AtomicCell<bool>,
    }

    struct TestLongDelayedOperation {
        should_complete: AtomicCell<bool>,
        completed: AtomicCell<bool>,
    }

    impl TestShortDelayedOperation {
        fn new(should_complete: bool) -> Self {
            Self {
                should_complete: AtomicCell::new(should_complete),
                completed: AtomicCell::new(false),
            }
        }
    }
    impl TestLongDelayedOperation {
        fn new(should_complete: bool) -> Self {
            Self {
                should_complete: AtomicCell::new(should_complete),
                completed: AtomicCell::new(false),
            }
        }
    }

    impl DelayedAsyncOperation for TestShortDelayedOperation {
        fn delay_ms(&self) -> u64 {
            3 * 1000
        }

        async fn try_complete(&self) -> bool {
            self.should_complete.load()
        }

        fn on_complete(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                self.completed.store(true);
            })
        }

        async fn on_expiration(&self) {}
    }
    impl DelayedAsyncOperation for TestLongDelayedOperation {
        fn delay_ms(&self) -> u64 {
            10 * 1000
        }

        async fn try_complete(&self) -> bool {
            self.should_complete.load()
        }

        fn on_complete(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(async move {
                self.completed.store(true);
            })
        }

        async fn on_expiration(&self) {}
    }

    #[rstest]
    #[tokio::test]
    async fn test_try_complete_else_watch(_setup: ()) {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, _) = mpsc::channel(1);

        let purgatory = DelayedAsyncOperationPurgatory::<TestShortDelayedOperation>::new(
            "test",
            notify_shutdown,
            shutdown_complete_tx,
        )
        .await;

        // 测试立即完成的操作
        let op = Arc::new(TestShortDelayedOperation::new(true));
        let op_clone = Arc::clone(&op);
        let completed = purgatory
            .try_complete_else_watch(op_clone, vec!["test_key".to_string()])
            .await;
        assert!(completed);
        assert!(op.completed.load());

        // 测试需要watch的操作
        let op = Arc::new(TestShortDelayedOperation::new(false));
        let op_clone = Arc::clone(&op);
        let completed = purgatory
            .try_complete_else_watch(op_clone, vec!["test_key".to_string()])
            .await;
        assert!(!completed);

        // 验证watchers中有一个操作
        assert_eq!(purgatory.watchers.get("test_key").unwrap().len(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_check_and_complete(_setup: ()) {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, _) = mpsc::channel(1);

        let purgatory = DelayedAsyncOperationPurgatory::<TestShortDelayedOperation>::new(
            "test",
            notify_shutdown,
            shutdown_complete_tx,
        )
        .await;

        // 添加两个操作到watchers
        let op1 = Arc::new(TestShortDelayedOperation::new(false));
        let op2 = Arc::new(TestShortDelayedOperation::new(false));
        let op1_clone = Arc::clone(&op1);
        let op2_clone = Arc::clone(&op2);

        purgatory
            .try_complete_else_watch(op1_clone, vec!["test_key".to_string()])
            .await;
        purgatory
            .try_complete_else_watch(op2_clone, vec!["test_key".to_string()])
            .await;

        // 验证初始状态
        assert_eq!(purgatory.watchers.get("test_key").unwrap().len(), 2);

        // 修改第一个操作为可完成
        purgatory.watchers.get("test_key").unwrap()[0]
            .operation
            .should_complete
            .store(true);

        // 检查完成状态
        let completed = purgatory.check_and_complete("test_key").await;
        assert_eq!(completed, 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_purge_completed(_setup: ()) {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, _) = mpsc::channel(1);

        let purgatory = DelayedAsyncOperationPurgatory::<TestShortDelayedOperation>::new(
            "test",
            notify_shutdown,
            shutdown_complete_tx,
        )
        .await;

        // 添加一个已完成的操作
        let op = Arc::new(TestShortDelayedOperation::new(false));
        let op_clone = Arc::clone(&op);
        // 加进watchers
        purgatory
            .try_complete_else_watch(op_clone, vec!["test_key".to_string()])
            .await;

        // 修改第一个操作为可完成
        purgatory.watchers.get("test_key").unwrap()[0]
            .operation
            .should_complete
            .store(true);

        let completed = purgatory.check_and_complete("test_key").await;
        assert_eq!(completed, 1);
        // 清理已完成的操作
        purgatory.purge_completed().await;

        // 验证watchers为空
        assert!(purgatory.watchers.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn test_operation_expiration(_setup: ()) {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, _) = mpsc::channel(1);

        let short_delay_purgatory =
            DelayedAsyncOperationPurgatory::<TestShortDelayedOperation>::new(
                "test",
                notify_shutdown.clone(),
                shutdown_complete_tx.clone(),
            )
            .await;

        let long_delay_purgatory = DelayedAsyncOperationPurgatory::<TestLongDelayedOperation>::new(
            "test",
            notify_shutdown.clone(),
            shutdown_complete_tx.clone(),
        )
        .await;

        // 添加一个短超时的操作
        let short_delay_op = Arc::new(TestShortDelayedOperation::new(false));
        let short_delay_op_clone = Arc::clone(&short_delay_op);
        short_delay_purgatory
            .try_complete_else_watch(short_delay_op_clone, vec!["test_key1".to_string()])
            .await;

        // 添加一个长超时的操作
        let long_delay_op = Arc::new(TestLongDelayedOperation::new(false));
        let long_delay_op_clone = Arc::clone(&long_delay_op);
        long_delay_purgatory
            .try_complete_else_watch(long_delay_op_clone, vec!["test_key2".to_string()])
            .await;

        // 等待第一个操作超时
        tokio::time::sleep(Duration::from_millis(4000)).await;
        assert!(short_delay_purgatory.watchers.get("test_key1").unwrap()[0]
            .is_expired
            .load());
        assert!(!long_delay_purgatory.watchers.get("test_key2").unwrap()[0]
            .is_expired
            .load());

        // 提前完成第二个操作
        long_delay_op.should_complete.store(true);
        let completed = long_delay_purgatory.check_and_complete("test_key2").await;
        assert_eq!(completed, 1);
        assert!(long_delay_op.completed.load());
    }
}
