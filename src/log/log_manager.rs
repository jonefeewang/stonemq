use crate::log::{CheckPointFile, JournalLog, RECOVERY_POINT_FILE_NAME, SPLIT_POINT_FILE_NAME};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::Interval;

use crate::message::TopicPartition;
use crate::{global_config, AppError, AppResult, Shutdown};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info, trace, warn};

use super::queue_log::QueueLog;
use super::splitter::SplitterTask;

///
/// 这里使用DashMap来保障并发安全，但是安全仅限于对map entry的增加或删除。对于log的读写操作，则需要tokio RwLock
/// 来保护。
/// 1. 对于partition的增加或减少，这种操作相对低频，这里的DashMap保障读写锁，锁争抢的概率较低，代价是可以接受的
/// 2. 对于log的读写操作，这里的RwLock保障并发读写。读操作直接使用log的不可变
///
///
#[derive(Debug)]
pub struct LogManager {
    journal_logs: DashMap<TopicPartition, Arc<JournalLog>>,
    queue_logs: DashMap<TopicPartition, Arc<QueueLog>>,
    journal_recovery_checkpoints: CheckPointFile,
    queue_recovery_checkpoints: CheckPointFile,
    split_checkpoint: CheckPointFile,
    notify_shutdown: broadcast::Sender<()>,
    _shutdown_complete_tx: Sender<()>,
    journal_log_path: String,
    queue_log_path: String,
}

impl LogManager {
    pub fn new(notify_shutdown: broadcast::Sender<()>, shutdown_complete_tx: Sender<()>) -> Self {
        let journal_recovery_checkpoint_path = format!(
            "{}/{}",
            global_config().log.journal_base_dir,
            RECOVERY_POINT_FILE_NAME
        );

        let queue_recovery_checkpoint_path = format!(
            "{}/{}",
            global_config().log.queue_base_dir,
            RECOVERY_POINT_FILE_NAME
        );
        let split_checkpoint_path = format!(
            "{}/{}",
            global_config().log.journal_base_dir,
            SPLIT_POINT_FILE_NAME
        );
        LogManager {
            journal_logs: DashMap::new(),
            queue_logs: DashMap::new(),
            journal_recovery_checkpoints: CheckPointFile::new(journal_recovery_checkpoint_path),
            queue_recovery_checkpoints: CheckPointFile::new(queue_recovery_checkpoint_path),
            split_checkpoint: CheckPointFile::new(split_checkpoint_path),
            notify_shutdown,
            _shutdown_complete_tx: shutdown_complete_tx,
            journal_log_path: global_config().log.journal_base_dir.clone(),
            queue_log_path: global_config().log.queue_base_dir.clone(),
        }
    }

    ///
    /// 在broker启动的时候，从硬盘加载所有的日志文件，包括journal和queue日志
    /// 预期在broker启动前加载
    pub async fn startup(mut self) -> AppResult<Arc<LogManager>> {
        info!("log manager startup ...");
        let log_config = &global_config().log;
        let journal_index_file_size = log_config.journal_index_file_size as u32;
        let queue_index_file_size = log_config.queue_index_file_size as u32;
        let journal_logs = self.load_journal_logs(journal_index_file_size).await?;
        self.journal_logs.extend(journal_logs);
        let queue_logs = self.load_queue_logs(queue_index_file_size).await?;
        self.queue_logs.extend(queue_logs);

        // startup background tasks
        let log_manager = Arc::new(self);
        log_manager.clone().start_task().await?;
        info!("log manager startup completed.");
        Ok(log_manager)
    }

    pub async fn load_journal_logs(
        &self,
        index_file_max_size: u32,
    ) -> AppResult<Vec<(TopicPartition, Arc<JournalLog>)>> {
        info!("load journal logs from {}", self.journal_log_path);

        if !PathBuf::from(&self.journal_log_path).exists() {
            error!("journal log path not exist:{}", self.journal_log_path);
            return Err(AppError::IllegalStateError(format!(
                "journal log path not exist:{}",
                self.journal_log_path.clone(),
            )));
        }

        let logs = self.do_load_journal_log(index_file_max_size).await?;
        info!(
            "load {} logs from dir:{} finished",
            logs.len(),
            self.journal_log_path
        );
        Ok(logs)
    }

    async fn do_load_journal_log(
        &self,
        index_file_max_size: u32,
    ) -> AppResult<Vec<(TopicPartition, Arc<JournalLog>)>> {
        // JournalLog 要加载recovery_checkpoints和split_checkpoint，还有queue_log的next_offset_checkpoint
        let recovery_checkpoints = self.journal_recovery_checkpoints.read_checkpoints().await?;
        let split_checkpoints = self.split_checkpoint.read_checkpoints().await?;

        let mut logs = vec![];
        let mut dir = fs::read_dir(&self.journal_log_path).map_err(|e| {
            AppError::DetailedIoError(format!("read journal log path error: {}", e))
        })?;
        while let Some(dir) = dir
            .next()
            .transpose()
            .map_err(|e| AppError::DetailedIoError(format!("read journal log path error: {}", e)))?
        {
            let file_type = dir
                .metadata()
                .map_err(|e| {
                    AppError::DetailedIoError(format!("read journal log path error: {}", e))
                })?
                .file_type();
            if file_type.is_dir() {
                debug!("load journal log for dir:{}", dir.path().to_string_lossy());
                let tp = TopicPartition::from_string(dir.file_name().to_string_lossy())?;
                let split_offset = split_checkpoints.get(&tp).unwrap_or(&-1).to_owned();
                let recovery_offset = recovery_checkpoints.get(&tp).unwrap_or(&0).to_owned();
                let log = JournalLog::load_from(
                    &tp,
                    recovery_offset,
                    split_offset,
                    dir.path(),
                    index_file_max_size,
                )
                .await?;
                logs.push((tp, Arc::new(log)));
            } else if dir.file_name().to_string_lossy().ends_with("checkpoints") {
                trace!("skip recovery file: {:?}", dir.path().to_string_lossy());
            } else {
                warn!("invalid log dir:{:?}", dir.path().to_string_lossy());
            }
        }
        Ok(logs)
    }

    pub async fn load_queue_logs(
        &self,
        index_file_max_size: u32,
    ) -> AppResult<Vec<(TopicPartition, Arc<QueueLog>)>> {
        info!("load queue logs from {}", self.queue_log_path);

        if !PathBuf::from(&self.queue_log_path).exists() {
            error!("queue log path not exist:{}", self.queue_log_path);
            return Err(AppError::IllegalStateError(format!(
                "queue log path not exist:{}",
                self.queue_log_path.clone(),
            )));
        }

        let logs = self.do_load_queue_logs(index_file_max_size).await?;
        info!(
            "load {} logs from dir:{} finished",
            logs.len(),
            self.queue_log_path
        );
        Ok(logs)
    }

    async fn do_load_queue_logs(
        &self,
        index_file_max_size: u32,
    ) -> AppResult<Vec<(TopicPartition, Arc<QueueLog>)>> {
        // 加载检查点文件
        let recovery_checkpoints = self.queue_recovery_checkpoints.read_checkpoints().await?;

        let mut logs = vec![];
        let mut dir = fs::read_dir(&self.queue_log_path)
            .map_err(|e| AppError::DetailedIoError(format!("read queue log path error: {}", e)))?;
        while let Some(dir) = dir
            .next()
            .transpose()
            .map_err(|e| AppError::DetailedIoError(format!("read queue log path error: {}", e)))?
        {
            let file_type = dir
                .metadata()
                .map_err(|e| {
                    AppError::DetailedIoError(format!("read queue log path error: {}", e))
                })?
                .file_type();
            if file_type.is_dir() {
                let tp = TopicPartition::from_string(dir.file_name().to_string_lossy())?;
                let recovery_offset = recovery_checkpoints.get(&tp).unwrap_or(&0).to_owned();
                let log = QueueLog::load_from(&tp, recovery_offset, index_file_max_size).await?;

                trace!("found log:{:}", &tp.id());
                logs.push((tp, Arc::new(log)));
            } else {
                warn!("invalid log dir:{:?}", dir.path().to_string_lossy());
            }
        }
        Ok(logs)
    }

    pub async fn get_or_create_journal_log(
        &self,
        topic_partition: &TopicPartition,
    ) -> AppResult<Arc<JournalLog>> {
        let log = self.journal_logs.entry(topic_partition.clone());
        match log {
            Entry::Occupied(occupied) => Ok(occupied.get().clone()),
            Entry::Vacant(vacant) => {
                info!(
                    "create journal log for topic-partition:{}",
                    topic_partition.id()
                );

                let journal_log = JournalLog::new(topic_partition).await?;
                let log = Arc::new(journal_log);
                vacant.insert(log.clone());
                Ok(log)
            }
        }
    }
    pub async fn get_or_create_queue_log(
        &self,
        topic_partition: &TopicPartition,
    ) -> AppResult<Arc<QueueLog>> {
        let log = self.queue_logs.entry(topic_partition.clone());

        match log {
            Entry::Occupied(occupied) => Ok(occupied.get().clone()),
            Entry::Vacant(vacant) => {
                let log = Arc::new(QueueLog::new(topic_partition).await?);
                vacant.insert(log.clone());
                Ok(log)
            }
        }
    }
    async fn recovery_checkpoint_task(
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
                    (tp.clone(), log.recover_point.load())
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
    pub async fn start_task(self: Arc<Self>) -> AppResult<()> {
        let recovery_check_interval = global_config().log.recovery_checkpoint_interval;
        let interval = tokio::time::interval(Duration::from_secs(recovery_check_interval));
        let shutdown = Shutdown::new(self.notify_shutdown.subscribe());
        tokio::spawn(async move {
            {
                let result = self.recovery_checkpoint_task(interval, shutdown).await;
                match result {
                    Ok(_) => {
                        trace!("journal log recovery checkpoint task shutdown");
                    }
                    Err(error) => {
                        error!("recovery checkpoint task error:{:?}", error);
                    }
                }
            }
        });
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
}

impl Drop for LogManager {
    fn drop(&mut self) {
        debug!("log manager dropped");
    }
}
