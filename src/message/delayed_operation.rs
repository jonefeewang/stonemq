use crossbeam_utils::atomic::AtomicCell;
use dashmap::{DashMap, DashSet};
use futures_util::ready;
use tracing::{error, info};

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::{Stream, StreamExt};
use tokio_util::time::DelayQueue;

use tokio::sync::broadcast;
use tokio::time::Duration;

use std::collections::HashMap;
use std::hash::Hash;
use tokio::time::interval;

use crate::Shutdown;

pub trait DelayedOperation: Send + Sync + Debug + Eq + Hash {
    /// 尝试是否可以马上完成延迟操作，如果可以,则调用force_complete,
    /// 并返回force_complete的返回值, 否则返回false
    ///
    /// # 返回值
    ///
    /// 如果可以马上执行，这返回force_complete的返回值, 否则返回false
    ///
    /// # 异步
    ///
    /// 此方法是异步的，需要使用 `.await` 来等待其完成。
    async fn try_complete(&self) -> bool;
    /// 当延迟操作完成时调用此方法，这个方法在具体的延迟操作中实现
    /// 这个方法只会在force_complete中调用一次
    ///
    /// # 异步
    ///
    /// 此方法是异步的，需要使用 `.await` 来等待其完成。
    fn on_complete(&self) -> impl Future<Output = ()> + Send;

    /// 当延迟操作过期时调用此方法，这个方法在具体的延迟操作中实现
    /// 这个方法只会在操作过期后，系统调用完force_complete后调用
    ///
    /// # 异步
    ///
    /// 此方法是异步的，需要使用 `.await` 来等待其完成。
    fn on_expiration(&self) -> impl Future<Output = ()> + Send;

    fn cancel(&self) -> impl Future<Output = ()> + Send;

    /// 获取延迟操作的延迟时间
    fn delay_ms(&self) -> u64;

    fn is_completed(&self) -> &AtomicCell<bool>;

    fn op_id(&self) -> String;

    /// 强制完成延迟操作
    ///
    /// 此方法尝试将操作标记为已完成，并执行相关的完成操作。
    ///
    /// 1. 操作在tryComplete()中被验证为可完成
    /// 2. 操作已过期，因此需要立即完成
    ///
    /// # 返回值
    ///
    /// - 如果操作成功标记为已完成，返回 `true`，表示操作由当前线程完成
    /// - 返回 `false`，表示操作已经由其他线程完成
    ///
    /// # 行为
    ///
    /// 1. 尝试将 `completed` 状态从 `false` 更改为 `true`
    /// 2. 如果更改成功：
    ///    - 调用 `cancel()` 方法
    ///    - 调用 `on_complete()` 方法
    ///    - 返回 `true`
    /// 3. 如果更改失败（即操作已经完成），返回 `false`
    ///
    /// # 异步
    ///
    /// 此方法是异步的，需要使用 `.await` 来等待其完成。
    fn force_complete(&self, cancel_timeout: bool) -> impl Future<Output = bool> + Send {
        async move {
            if self.is_completed().compare_exchange(false, true).is_ok() {
                if cancel_timeout {
                    self.cancel().await;
                }
                self.on_complete().await;
                true
            } else {
                false
            }
        }
    }

    fn timeout(&self) -> impl Future<Output = ()> + Send {
        // 修改这里
        async move {
            if self.force_complete(false).await {
                self.on_expiration().await;
            }
        }
    }
}

#[derive(Debug)]
pub struct DelayedOperationPurgatory<T: DelayedOperation + 'static> {
    name: String,
    watchers: DashMap<String, Arc<Watchers<T>>>,
    shutdown_complete_tx: Sender<()>,
    timer: Arc<Timer<T>>,
}

impl<T: DelayedOperation + Send + Sync + 'static> DelayedOperationPurgatory<T> {
    pub fn new(
        name: String,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: Sender<()>,
    ) -> Arc<Self> {
        let shutdown = Shutdown::new(notify_shutdown.subscribe());
        let purgatory = Arc::new(DelayedOperationPurgatory {
            name,
            watchers: DashMap::new(),
            shutdown_complete_tx,
            timer: Arc::new(Timer::new(shutdown)),
        });

        purgatory
    }

    pub async fn try_complete_else_watch(&self, operation: T, watch_keys: Vec<String>) -> bool {
        if operation.try_complete().await {
            return true;
        }

        let op = Arc::new(operation);

        for key in watch_keys {
            self.watch_for_operation(key, op.clone()).await;
        }

        if op.try_complete().await {
            return true;
        }

        if !op.is_completed().load() {
            self.timer
                .add(op.op_id(), op.clone(), Duration::from_millis(op.delay_ms()))
                .await;
            if op.is_completed().load() {
                self.cancel_timeout(op).await;
            }
        }

        false
    }

    // 检查某个key是否可以完成
    pub async fn check_and_complete(&self, key: String) -> usize {
        let watcher = self.watchers.get(&key);
        if let Some(watcher) = watcher {
            let watcher = watcher.clone();
            watcher.try_complete_watched().await
        } else {
            0
        }
    }

    async fn watch_for_operation(&self, key: String, operation: Arc<T>) {
        let watcher = self
            .watchers
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Watchers::new(key)));
        watcher.watch(operation)
    }

    pub async fn cancel_timeout(&self, operation: Arc<T>) {
        self.timer.remove(operation.op_id()).await;
    }
    pub fn timer(&self) -> Arc<Timer<T>> {
        self.timer.clone()
    }
}

#[derive(Debug)]
struct Watchers<T: DelayedOperation> {
    key: String,
    operations: DashSet<Arc<T>>,
}

impl<T: DelayedOperation> Watchers<T> {
    fn new(key: String) -> Self {
        Watchers {
            key,
            operations: DashSet::new(),
        }
    }

    fn watch(&self, operation: Arc<T>) {
        self.operations.insert(operation);
    }

    pub async fn try_complete_watched(&self) -> usize {
        let mut completed_count = 0;
        let mut operations_to_remove = Vec::new();

        for operation in self.operations.iter() {
            let operation = operation.clone();
            if operation.is_completed().load() {
                operations_to_remove.push(operation.clone());
            } else if operation.try_complete().await {
                completed_count += 1;
                operations_to_remove.push(operation.clone());
            }
        }

        for operation in operations_to_remove {
            self.operations.remove(&operation);
        }

        completed_count
    }
}

struct QueueWarpper {
    queue: DelayQueue<String>,
}

impl Stream for QueueWarpper {
    type Item = String;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_mut = self.get_mut();

        match ready!(self_mut.queue.poll_expired(cx)) {
            Some(expired) => Poll::Ready(Some(expired.into_inner())),
            None => Poll::Ready(None),
        }
    }
}

#[derive(Debug)]
pub struct Timer<T: DelayedOperation + Send + Sync + 'static> {
    tx: mpsc::Sender<TimerMessage<T>>,
}

enum TimerMessage<T: DelayedOperation + Send + Sync + 'static> {
    Add(String, Arc<T>, Duration),
    Remove(String),
}

impl<T: DelayedOperation + Send + Sync + 'static> Timer<T> {
    pub fn new(mut shutdown: Shutdown) -> Self {
        let (tx, rx) = mpsc::channel(100);
        let timer = Timer { tx };
        tokio::spawn(async move {
            tokio::select! {
                _ = Timer::run(rx) => {},
                _ = shutdown.recv() => {
                    info!("Timer shutdown");
                },
            }
        });
        timer
    }

    pub async fn add(&self, key: String, item: Arc<T>, delay: Duration) {
        let _ = self.tx.send(TimerMessage::Add(key, item, delay)).await;
    }

    pub async fn remove(&self, key: String) {
        let _ = self.tx.send(TimerMessage::Remove(key)).await;
    }

    async fn run(mut rx: mpsc::Receiver<TimerMessage<T>>) {
        let mut queue_wrapper = QueueWarpper {
            queue: DelayQueue::new(),
        };
        let mut entries = HashMap::new();
        let mut operation_map = HashMap::new();
        let mut interval = interval(Duration::from_millis(100));

        loop {
            // 处理最多10条消息
            for _ in 0..10 {
                match rx.try_recv() {
                    Ok(msg) => match msg {
                        TimerMessage::Add(str_key, item, delay) => {
                            if let Some(delay_key) = entries.get(&str_key) {
                                queue_wrapper.queue.remove(delay_key);
                                error!("item already in queue");
                            }
                            let delay_key = queue_wrapper.queue.insert(str_key.clone(), delay);
                            entries.insert(str_key.clone(), delay_key);
                            operation_map.insert(str_key.clone(), item);
                        }
                        TimerMessage::Remove(str_key) => {
                            if let Some(delay_key) = entries.remove(&str_key) {
                                queue_wrapper.queue.remove(&delay_key);
                                operation_map.remove(&str_key);
                            }
                        }
                    },
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => return,
                }
            }

            // 检查并处理过期的项
            interval.tick().await;
            while let Some(expired) = queue_wrapper.next().await {
                if let Some(item) = operation_map.remove(&expired) {
                    item.timeout().await;
                }
            }
        }
    }
}
