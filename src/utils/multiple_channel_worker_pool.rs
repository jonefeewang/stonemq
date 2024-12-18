use std::any::type_name;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, error, trace, warn};

use crate::Shutdown;

/// Worker Pool Config Parameters
#[derive(Debug, Clone)]
pub struct WorkerPoolConfig {
    /// Channel Capacity
    pub channel_capacity: usize,
    /// Channel Number
    pub num_channels: i8,
    /// Monitor Interval
    pub monitor_interval: Duration,
    /// Worker Check Timeout param
    pub worker_check_timeout: Duration,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 1024,
            num_channels: 4,
            monitor_interval: Duration::from_secs(5),
            worker_check_timeout: Duration::from_millis(200),
        }
    }
}

/// represent a worker pool with multiple independent task channels
/// each channel has its own dedicated worker to ensure sequential processing
#[derive(Debug)]
pub struct MultipleChannelWorkerPool<T> {
    notify_shutdown: broadcast::Sender<()>,
    _shutdown_complete_tx: mpsc::Sender<()>,
    channels: Arc<HashMap<i8, TaskChannel<T>>>,
}
/// represent a task channel
#[derive(Debug)]
struct TaskChannel<T> {
    sender: async_channel::Sender<T>,
    receiver: async_channel::Receiver<T>,
}

/// represent a running worker
#[derive(Debug)]
struct Worker {
    id: i8,
    handle: JoinHandle<()>,
}

impl<T: Send + Debug + 'static> MultipleChannelWorkerPool<T> {
    pub fn new<F, Fut>(
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
        handler: F,
        config: WorkerPoolConfig,
    ) -> Self
    where
        F: Fn(T) -> Fut + Send + Sync + 'static + Clone,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let channels = Self::spawn_channels_with_monitor(config, notify_shutdown.clone(), handler);

        Self {
            notify_shutdown,
            _shutdown_complete_tx: shutdown_complete_tx,
            channels,
        }
    }
    /// Send request to specified channel
    pub async fn send(
        &self,
        request: T,
        channel_id: i8,
    ) -> Result<(), async_channel::SendError<T>> {
        self.channels
            .get(&channel_id)
            .expect("channel not found")
            .sender
            .send(request)
            .await
    }

    /// Get channel count
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    fn spawn_channels_with_monitor<F, Fut>(
        config: WorkerPoolConfig,
        notify_shutdown: broadcast::Sender<()>,
        handler: F,
    ) -> Arc<HashMap<i8, TaskChannel<T>>>
    where
        F: Fn(T) -> Fut + Send + Sync + 'static + Clone,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut workers = Vec::with_capacity(config.num_channels as usize);
        let mut channels = HashMap::with_capacity(config.num_channels as usize);

        // Create a dedicated worker for each channel
        for id in 0..config.num_channels {
            let (sender, receiver) = async_channel::bounded(config.channel_capacity);
            let worker = Self::spawn_worker(
                id,
                handler.clone(),
                notify_shutdown.clone(),
                receiver.clone(),
            );
            workers.push(worker);
            channels.insert(
                id,
                TaskChannel {
                    sender,
                    receiver: receiver.clone(),
                },
            );
        }

        let channels = Arc::new(channels);
        let channels_clone = channels.clone();

        // Start monitor
        Self::spawn_monitor(workers, channels_clone, notify_shutdown, handler, config);

        channels
    }

    fn spawn_worker<F, Fut>(
        id: i8,
        handler: F,
        notify_shutdown: broadcast::Sender<()>,
        receiver: async_channel::Receiver<T>,
    ) -> Worker
    where
        F: Fn(T) -> Fut + Send + 'static + Clone,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let mut shutdown = Shutdown::new(notify_shutdown.subscribe());

        let handle = tokio::spawn(async move {
            debug!("Worker {id} started");

            loop {
                tokio::select! {
                    Ok(request) = receiver.recv() => {
                        handler(request).await;
                    }
                    _ = shutdown.recv() => {
                        debug!("Worker {id} shutting down");
                        break;
                    }
                }
            }
        });

        Worker { id, handle }
    }

    fn spawn_monitor<F, Fut>(
        mut workers: Vec<Worker>,
        channels: Arc<HashMap<i8, TaskChannel<T>>>,
        notify_shutdown: broadcast::Sender<()>,
        handler: F,
        config: WorkerPoolConfig,
    ) where
        F: Fn(T) -> Fut + Send + 'static + Clone,
        Fut: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(async move {
            let mut interval = time::interval(config.monitor_interval);
            let mut shutdown = Shutdown::new(notify_shutdown.subscribe());

            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        debug!("Worker monitor received shutdown signal");
                        break;
                    }
                    _ = interval.tick() => {
                        for worker in &mut workers {
                            match time::timeout(config.worker_check_timeout, &mut worker.handle).await {
                                Ok(join_result) => {
                                    match join_result {
                                        Ok(_) => {
                                            warn!("Worker {} completed unexpectedly", worker.id);
                                        }
                                        Err(err) => {
                                            if err.is_panic() {
                                                Self::log_worker_panic(worker.id, err);
                                            } else {
                                                error!("Worker {} failed with non-panic error", worker.id);
                                            }
                                        }
                                    }

                                    warn!("Worker {} failed, restarting...", worker.id);
                                    // 重启 worker
                                    *worker = Self::spawn_worker(
                                        worker.id,
                                        handler.clone(),
                                        notify_shutdown.clone(),
                                        channels.get(&worker.id).unwrap().receiver.clone(),
                                    );
                                    debug!("Worker {} restarted", worker.id);
                                }
                                Err(_) => {
                                    trace!("Worker {} is running", worker.id);
                                }
                            }
                        }
                    }
                }
            }
            debug!("Worker monitor exiting");
        });
    }

    fn log_worker_panic(worker_id: i8, err: tokio::task::JoinError) {
        let payload = err.into_panic();
        if let Some(message) = payload.downcast_ref::<&'static str>() {
            error!("Worker {worker_id} panicked with message: {message}");
        } else if let Some(message) = payload.downcast_ref::<String>() {
            error!("Worker {worker_id} panicked with message: {message}");
        } else {
            error!(
                "Worker {worker_id} panicked with an unknown type: {}",
                get_type_name(&payload)
            );
        }
    }
}

#[inline]
fn get_type_name<R>(_: &R) -> &'static str {
    type_name::<R>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_custom_config() {
        let (notify_shutdown, _) = broadcast::channel(1);
        let (shutdown_complete_tx, _) = mpsc::channel(1);
        let config = WorkerPoolConfig::default();

        let pool = MultipleChannelWorkerPool::new(
            notify_shutdown,
            shutdown_complete_tx,
            |msg: String| async move {
                println!("Handling message: {}", msg);
            },
            config,
        );

        // Send message to different channels
        pool.send("test message 1".to_string(), 0).await.unwrap();
        pool.send("test message 2".to_string(), 1).await.unwrap();
    }
}
