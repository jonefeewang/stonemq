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

use std::any::type_name;
use std::fmt::Debug;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, error, warn};

use crate::Shutdown;

pub struct WorkerPool<T, F, Fut>
where
    T: Send + 'static,
    F: Fn(T) -> Fut + Send + 'static + Clone,
    Fut: Future<Output = ()> + Send + 'static,
{
    request_tx: async_channel::Sender<T>,
    notify_shutdown: broadcast::Sender<()>,
    _shutdown_complete_tx: mpsc::Sender<()>,
    worker_count: AtomicUsize,
    _phantom: std::marker::PhantomData<(F, Fut)>,
}

#[allow(dead_code)]
struct Worker {
    id: usize,
    handle: JoinHandle<()>,
}
#[allow(dead_code)]
impl<T, F, Fut> WorkerPool<T, F, Fut>
where
    T: Send + Debug + 'static,
    F: Fn(T) -> Fut + Send + 'static + Clone,
    Fut: Future<Output = ()> + Send + 'static,
{
    pub fn new(
        capacity: usize,
        num_workers: usize,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: mpsc::Sender<()>,
        handler: F,
    ) -> Self {
        let (request_tx, request_rx) = async_channel::bounded(capacity);
        let pool = Self {
            request_tx,
            notify_shutdown: notify_shutdown.clone(),
            _shutdown_complete_tx: shutdown_complete_tx.clone(),
            worker_count: AtomicUsize::new(num_workers),
            _phantom: std::marker::PhantomData,
        };

        // start workers and monitor
        pool.spawn_workers_with_monitor(
            num_workers,
            request_rx,
            notify_shutdown,
            shutdown_complete_tx,
            handler,
        );

        pool
    }

    fn spawn_workers_with_monitor(
        &self,
        num_workers: usize,
        request_rx: async_channel::Receiver<T>,
        notify_shutdown: broadcast::Sender<()>,
        _shutdown_complete_tx: mpsc::Sender<()>,
        handler: F,
    ) {
        let mut workers = Vec::with_capacity(num_workers);

        // create initial workers
        for id in 0..num_workers {
            let worker = self.spawn_worker(id, request_rx.clone(), handler.clone());
            workers.push(worker);
        }

        // 启动monitor
        let pool = self.clone();
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(5));
            let mut shutdown = Shutdown::new(notify_shutdown.subscribe());

            loop {
                tokio::select! {
                    _ = shutdown.recv() => {
                        debug!("Worker monitor received shutdown signal");
                        break;
                    }
                    _ = interval.tick() => {
                        // 检查所有worker状态
                        for worker in &mut workers {

                            match time::timeout(Duration::from_millis(200), &mut worker.handle).await {
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
                                                        "Worker {} panicked with message: {}",
                                                        worker.id,
                                                        message
                                                    );
                                                } else if let Some(message) =
                                                    payload.downcast_ref::<String>()
                                                {
                                                    error!(
                                                        "Worker {} panicked with message: {}",
                                                        worker.id,
                                                        message
                                                    );
                                                } else {
                                                    // print dynamic type name
                                                    error!(
                                                        "Worker {} panicked with an unknown type: {}",
                                                        worker.id,
                                                        get_type_name(&payload)
                                                    );
                                                }
                                            }

                                            warn!("Worker {} failed, restarting...", worker.id);
                                            // restart worker
                                            *worker = pool.spawn_worker(
                                                worker.id,
                                                request_rx.clone(),
                                                handler.clone(),
                                            );

                                        }
                                    }
                                }
                                Err(_) => {
                                    // ignore, task is running
                                }
                            }
                        }
                    }
                }
            }
            debug!("Worker monitor exiting");
        });
    }

    fn spawn_worker(
        &self,
        id: usize,
        request_rx: async_channel::Receiver<T>,
        handler: F,
    ) -> Worker {
        let mut shutdown = Shutdown::new(self.notify_shutdown.subscribe());

        let handle = tokio::spawn(async move {
            debug!("Worker {} started", id);

            loop {
                tokio::select! {
                    Ok(request) = request_rx.recv() => {
                        handler(request).await;
                    }
                    _ = shutdown.recv() => {
                        debug!("Worker {} shutting down", id);
                        break;
                    }
                }
            }
        });

        Worker { id, handle }
    }

    pub async fn send(&self, request: T) -> Result<(), async_channel::SendError<T>> {
        self.request_tx.send(request).await
    }

    pub fn worker_count(&self) -> usize {
        self.worker_count.load(Ordering::Relaxed)
    }
}

impl<T, F, Fut> Clone for WorkerPool<T, F, Fut>
where
    T: Send + 'static,
    F: Fn(T) -> Fut + Send + 'static + Clone,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            request_tx: self.request_tx.clone(),
            notify_shutdown: self.notify_shutdown.clone(),
            _shutdown_complete_tx: self._shutdown_complete_tx.clone(),
            worker_count: AtomicUsize::new(self.worker_count.load(Ordering::Relaxed)),
            _phantom: std::marker::PhantomData,
        }
    }
}
fn get_type_name<R>(_: &R) -> &'static str {
    type_name::<R>()
}
