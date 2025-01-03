use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
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
                    log.flush().await.unwrap();
                }
                for entry in self.queue_logs.iter() {
                    info!(
                        "log manager is shutting down, flush queue log for topic-partition:{}",
                        entry.key().id()
                    );
                    let log = entry.value();
                    log.flush().await.unwrap();
                }
            }

            let check_points: HashMap<TopicPartition, i64> = self
                .journal_logs
                .iter()
                .map(|entry| {
                    let tp = entry.key();
                    let log = entry.value();
                    (tp.clone(), log.recover_point.load())
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

            // 写入queue的recovery_checkpoint
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

            let split_checkpoints: HashMap<TopicPartition, i64> = self
                .journal_logs
                .iter()
                .map(|entry| {
                    let tp = entry.key();
                    let log = entry.value();
                    debug!(
                        "split progress:{}/{}",
                        log.split_offset.load(),
                        log.next_offset.load()
                    );
                    (tp.clone(), log.split_offset.load())
                })
                .collect();
            self.split_checkpoint
                .write_checkpoints(split_checkpoints)
                .await
                .map_err(|e| {
                    AppError::DetailedIoError(format!("write split checkpoint error: {}", e))
                })?;

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

    pub fn init_active_segment_writer(notify_shutdown: broadcast::Sender<()>) {
        let worker_pool_config = WorkerPoolConfig {
            channel_capacity: global_config().active_segment_writer_pool.channel_capacity,
            num_channels: global_config().active_segment_writer_pool.num_channels,
            monitor_interval: Duration::from_millis(
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
