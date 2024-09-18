use crate::log::{
    CheckPointFile, JournalLog, Log, LogType, QueueLog, RECOVERY_POINT_FILE_NAME,
    SPLIT_POINT_FILE_NAME,
};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::any::type_name;

use crate::log::splitter::SplitterTask;
use crate::message::TopicPartition;
use crate::AppError::InvalidValue;
use crate::{global_config, log, AppResult, ReplicaManager, Shutdown};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender;
use tokio::time::Interval;
use tracing::{error, info, trace, warn};
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
    shutdown_complete_tx: Sender<()>,
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
            shutdown_complete_tx,
            journal_log_path: global_config().log.journal_base_dir.clone(),
            queue_log_path: global_config().log.queue_base_dir.clone(),
        }
    }

    ///
    /// 在broker启动的时候，从硬盘加载所有的日志文件，包括journal和queue日志
    /// 预期在broker启动前加载
    pub fn startup(mut self, rt: &Runtime) -> AppResult<Arc<LogManager>> {
        info!("log manager startup ...");

        let log_config = &global_config().log;
        let journal_logs =
            self.load_logs::<JournalLog>(&log_config.journal_base_dir, rt, LogType::JournalLog)?;
        self.journal_logs.extend(journal_logs);
        let queue_logs =
            self.load_logs::<QueueLog>(&log_config.queue_base_dir, rt, LogType::QueueLog)?;
        self.queue_logs.extend(queue_logs);

        // startup background tasks
        let log_manager = Arc::new(self);
        rt.block_on(log_manager.clone().start_task())?;
        info!("log manager startup completed.");
        Ok(log_manager)
    }

    pub fn load_logs<T: Log>(
        &self,
        logs_dir: &str,
        rt: &Runtime,
        log_type: LogType,
    ) -> AppResult<Vec<(TopicPartition, Arc<T>)>> {
        info!("load logs from dir:{} for {}", logs_dir, type_name::<T>());
        if !fs::metadata(logs_dir)
            .map(|meta| meta.is_dir())
            .unwrap_or(false)
        {
            let (msg, arg) = (
                "logs directory does not exist, lacks the necessary permissions, or is a file.: {}",
                logs_dir,
            );
            error!("{} {}", msg, arg);
            return Err(InvalidValue(msg, arg.to_string()));
        }

        // 加载check points file

        let recovery_checkpoints =
            rt.block_on(self.journal_recovery_checkpoints.read_checkpoints())?;
        let split_checkpoints = rt.block_on(self.split_checkpoint.read_checkpoints())?;

        let mut logs = vec![];
        let mut dir = fs::read_dir(logs_dir)?;
        while let Some(dir) = dir.next().transpose()? {
            let file_type = dir.metadata()?.file_type();
            if file_type.is_dir() {
                let tp = TopicPartition::from_string(dir.file_name().to_string_lossy())?;
                let split_offset = split_checkpoints.get(&tp).unwrap_or(&0).to_owned();
                let recovery_offset = recovery_checkpoints.get(&tp).unwrap_or(&0).to_owned();
                let log = LogManager::load_log(&tp, recovery_offset, split_offset, rt)?;

                trace!("found log:{:}", &tp.id());
                logs.push((tp, Arc::new(log)));
            } else {
                warn!("invalid log dir:{:?}", dir.path().to_string_lossy());
            }
        }
        info!("load {} logs from dir:{} finished", logs.len(), logs_dir);
        Ok(logs)
    }
    ///
    /// 加载单个topic-partition的日志目录,并加载其中的segment文件
    fn load_log<T: Log>(
        topic_partition: &TopicPartition,
        recovery_offset: u64,
        split_offset: u64,
        rt: &Runtime,
    ) -> AppResult<T> {
        // 加载log目录下的segment文件
        let log_segments = T::load_segments(topic_partition, recovery_offset, rt)?;
        // 构建Log
        let log_start_offset = log_segments.first_key_value().map(|(k, _)| *k).unwrap_or(0);
        let log = rt.block_on(T::new(
            topic_partition,
            log_segments,
            log_start_offset,
            recovery_offset,
            split_offset,
        ))?;
        Ok(log)
    }
    pub async fn get_or_create_journal_log(
        &self,
        topic_partition: &TopicPartition,
    ) -> AppResult<Arc<JournalLog>> {
        let log = self.journal_logs.entry(topic_partition.clone());
        match log {
            Entry::Occupied(occupied) => Ok(occupied.get().clone()),
            Entry::Vacant(vacant) => {
                warn!(
                    "journal log for topic-partition:{} not found",
                    topic_partition.id()
                );

                let journal_log =
                    JournalLog::new(topic_partition, BTreeMap::new(), 0, 0, 0).await?;
                let log = Arc::new(journal_log);
                vacant.insert(log.clone());
                Ok(log)
            }
        }
    }
    pub fn get_or_create_queue_log(
        &self,
        topic_partition: &TopicPartition,
        rt: &Runtime,
    ) -> AppResult<Arc<QueueLog>> {
        let log = self.queue_logs.entry(topic_partition.clone());
        match log {
            Entry::Occupied(occupied) => Ok(occupied.get().clone()),
            Entry::Vacant(vacant) => {
                let queue_log_path = format!(
                    "{}/{}",
                    global_config().log.queue_base_dir,
                    topic_partition.id()
                );
                let log = Arc::new(rt.block_on(QueueLog::new(
                    topic_partition,
                    BTreeMap::new(),
                    0,
                    0,
                    0,
                ))?);
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
                _ = shutdown.recv() => {trace!("receiving shutdown signal");}
            };

            let check_points: HashMap<TopicPartition, u64> = self
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
                .await?;
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
        journal_topic_partition: &TopicPartition,
        queue_topic_partition: &HashSet<TopicPartition>,
    ) -> AppResult<()> {
        let journal_log = self
            .journal_logs
            .get(journal_topic_partition)
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
        let splitter = SplitterTask::new(journal_log, queue_logs, journal_topic_partition.clone());
        let ret = tokio::spawn(async move { splitter.run().await });
        ret.await.unwrap()?;
        Ok(())
    }
}
