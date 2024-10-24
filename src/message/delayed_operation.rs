use crossbeam_utils::atomic::AtomicCell;
use dashmap::DashMap;
use futures_util::ready;
use tracing::error;

use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc::Sender;
use tokio_stream::{Stream, StreamExt};
use tokio_util::time::DelayQueue;

use tokio::sync::{broadcast, RwLock};
use tokio::time::Duration;

use std::collections::HashMap;
use std::hash::Hash;
use tokio::time::interval;

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
    async fn try_complete(&mut self) -> bool;
    /// 当延迟操作完成时调用此方法，这个方法在具体的延迟操作中实现
    /// 这个方法只会在force_complete中调用一次
    ///
    /// # 异步
    ///
    /// 此方法是异步的，需要使用 `.await` 来等待其完成。
    async fn on_complete(&mut self);

    /// 当延迟操作过期时调用此方法，这个方法在具体的延迟操作中实现
    /// 这个方法只会在操作过期后，系统调用完force_complete后调用
    ///
    /// # 异步
    ///
    /// 此方法是异步的，需要使用 `.await` 来等待其完成。
    async fn on_expiration(&self);

    /// 获取延迟操作的延迟时间
    fn delay_ms(&self) -> u64;

    fn is_completed(&mut self) -> &AtomicCell<bool>;

    fn cancel(&self);

    fn lock(&self) -> Option<&RwLock<()>>;

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
    async fn force_complete(&mut self, cancel_timeout: bool) -> bool {
        if self.is_completed().compare_exchange(false, true).is_ok() {
            if cancel_timeout {
                self.cancel();
            }
            self.on_complete().await;
            true
        } else {
            false
        }
    }

    async fn timeout(&mut self) {
        if self.force_complete(false).await {
            self.on_expiration().await;
        }
    }
}

#[derive(Debug)]
pub struct DelayedOperationPurgatory<T: DelayedOperation + 'static> {
    name: String,
    watchers: DashMap<String, Watchers<T>>,
    shutdown_complete_tx: Sender<()>,
    timer: Timer<T>,
}

impl<T: DelayedOperation + Send + Sync + 'static> DelayedOperationPurgatory<T> {
    pub fn new(
        name: String,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: Sender<()>,
    ) -> Arc<Self> {
        let purgatory = Arc::new(DelayedOperationPurgatory {
            name,
            watchers: DashMap::new(),
            shutdown_complete_tx,
            timer: Timer::new(),
        });

        purgatory
    }

    pub async fn try_complete_else_watch(
        &self,
        operation: &Arc<T>,
        watch_keys: Vec<String>,
    ) -> bool {
        if operation.try_complete().await {
            return true;
        }

        for key in watch_keys {
            if operation.is_completed().load() {
                return false;
            }
            self.watch_for_operation(key, operation.clone()).await;
        }

        if operation.try_complete().await {
            return true;
        }

        if !operation.is_completed().load() {
            self.timer
                .add(
                    operation.clone(),
                    Duration::from_millis(operation.delay_ms()),
                )
                .await;
            if operation.is_completed().load() {
                operation.cancel();
            }
        }

        false
    }

    async fn watch_for_operation(&self, key: String, operation: Arc<T>) {
        let mut watcher = self
            .watchers
            .entry(key.clone())
            .or_insert_with(|| Watchers::new(key));
        watcher.watch(operation);
    }

    pub async fn check_and_complete(&self, key: String) -> usize {
        let watcher = self.watchers.get_mut(&key);
        if let Some(mut watcher) = watcher {
            watcher.try_complete_watched().await
        } else {
            0
        }
    }
}

#[derive(Debug)]
struct Watchers<T: DelayedOperation> {
    key: String,
    /// watcher 在purgatory中被dashmap保护，这里不需要加锁
    operations: Vec<Arc<T>>,
}

impl<T: DelayedOperation> Watchers<T> {
    fn new(key: String) -> Self {
        Watchers {
            key,
            operations: Vec::new(),
        }
    }

    fn watch(&mut self, operation: Arc<T>) {
        self.operations.push(operation);
    }

    pub async fn try_complete_watched(&mut self) -> usize {
        let mut completed_count = 0;
        let mut i = 0;

        while i < self.operations.len() {
            if self.operations[i].is_completed().load() {
                self.operations.remove(i);
            } else if self.operations[i].maybe_try_complete().await {
                completed_count += 1;
                self.operations.remove(i);
            } else {
                i += 1;
            }
        }
        completed_count
    }
}

struct QueueWarpper<T: Send + Sync + 'static> {
    queue: DelayQueue<Arc<T>>,
}

impl<T: DelayedOperation + Send + Sync + 'static> Stream for QueueWarpper<T> {
    type Item = Arc<T>;

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
    Add(Arc<T>, Duration),
    Remove(Arc<T>),
}

impl<T: DelayedOperation + Send + Sync + 'static> Timer<T> {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(100);
        let timer = Timer { tx };
        tokio::spawn(Timer::run(rx));
        timer
    }

    pub async fn add(&self, item: Arc<T>, delay: Duration) {
        let _ = self.tx.send(TimerMessage::Add(item, delay)).await;
    }

    pub async fn remove(&self, item: Arc<T>) {
        let _ = self.tx.send(TimerMessage::Remove(item)).await;
    }

    async fn run(mut rx: mpsc::Receiver<TimerMessage<T>>) {
        let mut queue = QueueWarpper {
            queue: DelayQueue::<Arc<T>>::new(),
        };
        let mut entries = HashMap::new();
        let mut interval = interval(Duration::from_millis(100));

        loop {
            // 处理最多10条消息
            for _ in 0..10 {
                match rx.try_recv() {
                    Ok(msg) => match msg {
                        TimerMessage::Add(item, delay) => {
                            if let Some(key) = entries.get(&item) {
                                queue.queue.remove(&key);
                                error!("item already in queue");
                            }
                            let key = queue.queue.insert(item.clone(), delay);
                            entries.insert(item, key);
                        }
                        TimerMessage::Remove(item) => {
                            if let Some(key) = entries.remove(&item) {
                                queue.queue.remove(&key);
                            }
                        }
                    },
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => return,
                }
            }

            // 检查并处理过期的项
            interval.tick().await;
            while let Some(expired) = queue.next().await {
                let item = expired.clone();
                entries.remove(&item);
                // 在这里处理过期的项目
                println!("项目过期: {:?}", item);
            }
        }
    }
}

impl<T: DelayedOperation + Send + Sync + 'static> Default for Timer<T> {
    fn default() -> Self {
        Self::new()
    }
}
