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

//! Log Manager Background Tasks Implementation
//!
//! This module implements various background tasks that run as part of the LogManager:
//! - Recovery checkpoint task for persisting recovery progress
//! - Splitter task for converting journal logs to queue logs
//! - Active segment writer initialization
//!
//! # Task Types
//!
//! ## Recovery Checkpoint Task
//! Periodically saves the recovery progress of both journal and queue logs to disk.
//! This ensures that after a system restart, logs can be recovered from their last
//! known good state.
//!
//! ## Splitter Task
//! Handles the conversion of messages from journal format to queue format.
//! This task reads from journal logs and writes to appropriate queue logs based
//! on topic partitions.
//!
//! ## Active Segment Writer
//! Manages the active segments of logs, handling the actual writing of data to disk
//! in an efficient manner using buffering and background flushes.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use tokio::{
    sync::{broadcast, mpsc::Sender},
    time::Interval,
};
use tracing::{debug, info, trace};

use crate::{
    global_config,
    log::{splitter::SplitterTask, QueueLog, WriteConfig},
    message::TopicPartition,
    utils::WorkerPoolConfig,
    AppError, AppResult, Shutdown,
};

use super::LogManager;

impl LogManager {
    /// Runs the recovery checkpoint task that periodically saves recovery progress.
    ///
    /// This task performs several critical functions:
    /// 1. Saves journal log recovery points
    /// 2. Saves queue log recovery points
    /// 3. Saves split progress checkpoints
    /// 4. Updates next offset checkpoints
    /// 5. Handles graceful shutdown by flushing all logs
    ///
    /// # Arguments
    ///
    /// * `interval` - The interval at which to run checkpoints
    /// * `shutdown` - Shutdown signal receiver
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - Success if all checkpoints are written, error otherwise
    pub async fn recovery_checkpoint_task(
        &self,
        mut interval: Interval,
        mut shutdown: Shutdown,
    ) -> AppResult<()> {
        loop {
            tokio::select! {
                // 第一次运行定时任务，会马上结束
                _ = interval.tick() => {trace!("tick complete .")},
                _ = shutdown.recv() => {trace!("recovery checkpoint task receiving shutdown signal");}
            };
            if shutdown.is_shutdown() {
                for entry in self.journal_logs.iter() {
                    info!(
                        "log manager is shutting down, flush journal log for topic-partition:{}",
                        entry.key().id()
                    );
                    let log = entry.value();
                    log.close().await.unwrap();
                }
                for entry in self.queue_logs.iter() {
                    info!(
                        "log manager is shutting down, flush queue log for topic-partition:{}",
                        entry.key().id()
                    );
                    let log = entry.value();
                    log.close().await.unwrap();
                }
            }

            // journal recovery checkpoint
            let check_points: HashMap<TopicPartition, i64> = self
                .journal_logs
                .iter()
                .map(|entry| {
                    let tp = entry.key();
                    let log = entry.value();
                    (tp.clone(), log.recover_point.load(Ordering::Acquire))
                })
                .collect();
            self.journal_recovery_checkpoints
                .write_checkpoints(check_points)
                .await
                .map_err(|e| {
                    AppError::DetailedIoError(format!(
                        "write journal recovery checkpoint error: {}",
                        e
                    ))
                })?;

            // queue recovery checkpoint
            let queue_check_points: HashMap<TopicPartition, i64> = self
                .queue_logs
                .iter()
                .map(|entry| {
                    let tp = entry.key();
                    let log = entry.value();
                    (tp.clone(), log.get_recover_point())
                })
                .collect();
            self.queue_recovery_checkpoints
                .write_checkpoints(queue_check_points)
                .await
                .map_err(|e| {
                    AppError::DetailedIoError(format!(
                        "write queue recovery checkpoint error: {}",
                        e
                    ))
                })?;

            // split checkpoint
            let split_checkpoints: HashMap<TopicPartition, i64> = self
                .journal_logs
                .iter()
                .map(|entry| {
                    let tp = entry.key();
                    let log = entry.value();
                    debug!(
                        "split progress:{}/{}",
                        log.split_offset.load(Ordering::Acquire),
                        log.next_offset.load(Ordering::Acquire)
                    );
                    (tp.clone(), log.split_offset.load(Ordering::Acquire))
                })
                .collect();
            self.split_checkpoint
                .write_checkpoints(split_checkpoints)
                .await
                .map_err(|e| {
                    AppError::DetailedIoError(format!("write split checkpoint error: {}", e))
                })?;

            // checkpoint next offset
            for entry in self.journal_logs.iter() {
                let log = entry.value();
                log.checkpoint_next_offset().await?;
            }

            if shutdown.is_shutdown() {
                break;
            }
        }
        Ok(())
    }

    /// Starts a splitter task for converting journal logs to queue logs.
    ///
    /// The splitter task reads messages from a journal log and distributes them
    /// to appropriate queue logs based on their topic partitions. This is a
    /// critical component in the message processing pipeline.
    ///
    /// # Arguments
    ///
    /// * `journal_topic_partition` - Source journal topic partition
    /// * `queue_topic_partition` - Set of target queue topic partitions
    /// * `shutdown` - Shutdown signal receiver
    /// * `shutdown_complete_tx` - Channel to signal task completion
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - Success if splitter starts, error otherwise
    pub async fn start_splitter_task(
        &self,
        journal_topic_partition: TopicPartition,
        queue_topic_partition: HashSet<TopicPartition>,
        shutdown: Shutdown,
        shutdown_complete_tx: Sender<()>,
    ) -> AppResult<()> {
        let journal_log = self
            .journal_logs
            .get(&journal_topic_partition)
            .unwrap()
            .value()
            .clone();
        let queue_logs: BTreeMap<TopicPartition, Arc<QueueLog>> = queue_topic_partition
            .iter()
            .map(|tp| {
                let queue_log = self.queue_logs.get(tp).unwrap().value().clone();
                (tp.clone(), queue_log)
            })
            .collect();

        let read_wait_interval = global_config().log.splitter_wait_interval as u64;

        let read_wait_interval = tokio::time::interval(Duration::from_millis(read_wait_interval));
        let mut splitter = SplitterTask::new(
            journal_log,
            queue_logs,
            journal_topic_partition.clone(),
            read_wait_interval,
            shutdown_complete_tx,
        );

        splitter.run(shutdown).await?;
        // tokio::time::sleep(Duration::from_secs(10)).await;
        Ok(())
    }

    /// Initializes the active segment writer with configuration from global settings.
    ///
    /// The active segment writer is responsible for efficiently managing write
    /// operations to log segments. It uses buffering and background flushes to
    /// optimize I/O performance.
    ///
    /// # Configuration
    ///
    /// Uses the following global configuration:
    /// - Worker pool settings (channel capacity, number of channels, etc.)
    /// - Write buffer settings (capacity, flush interval)
    ///
    /// # Arguments
    ///
    /// * `notify_shutdown` - Channel for shutdown notifications
    pub fn init_active_segment_writer(notify_shutdown: broadcast::Sender<()>) {
        let worker_pool_config = WorkerPoolConfig {
            channel_capacity: global_config().active_segment_writer_pool.channel_capacity,
            num_channels: global_config().active_segment_writer_pool.num_channels,
            monitor_interval: Duration::from_secs(
                global_config().active_segment_writer_pool.monitor_interval,
            ),
            worker_check_timeout: Duration::from_millis(
                global_config()
                    .active_segment_writer_pool
                    .worker_check_timeout,
            ),
        };

        let write_config = WriteConfig {
            buffer_capacity: global_config().active_segment_writer.buffer_capacity,
            flush_interval: Duration::from_millis(
                global_config().active_segment_writer.flush_interval,
            ),
        };

        info!(
            "create active segment writer with buffer capacity:{}",
            write_config.buffer_capacity
        );

        crate::log::init_active_segment_writer(
            notify_shutdown,
            Some(worker_pool_config),
            Some(write_config),
        );
    }
}
