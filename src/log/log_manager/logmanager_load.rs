use std::{path::PathBuf, sync::Arc};

use dashmap::Entry;
use tracing::{debug, error, info, trace, warn};

use crate::{
    log::{JournalLog, QueueLog},
    message::TopicPartition,
    AppError, AppResult, LogType,
};

use super::LogManager;

impl LogManager {
    pub fn load_journal_logs(
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

        let logs = self.do_load_journal_log(index_file_max_size)?;
        info!(
            "load {} logs from dir:{} finished",
            logs.len(),
            self.journal_log_path
        );
        Ok(logs)
    }

    fn do_load_journal_log(
        &self,
        index_file_max_size: u32,
    ) -> AppResult<Vec<(TopicPartition, Arc<JournalLog>)>> {
        let recovery_checkpoints = self
            .journal_recovery_checkpoints
            .read_checkpoints(LogType::Journal)?;
        let split_checkpoints = self.split_checkpoint.read_checkpoints(LogType::Journal)?;

        let mut logs = Vec::with_capacity(10);
        let mut dir = std::fs::read_dir(&self.journal_log_path).map_err(|e| {
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
                let dir_name = dir.file_name().to_string_lossy().into_owned();
                let tp = TopicPartition::from_str(&dir_name, LogType::Journal)?;
                let split_offset = split_checkpoints.get(&tp).unwrap_or(&-1).to_owned();
                let recovery_offset = recovery_checkpoints.get(&tp).unwrap_or(&0).to_owned();
                let log = JournalLog::load_from(
                    &tp,
                    recovery_offset,
                    split_offset,
                    dir.path(),
                    index_file_max_size,
                )?;
                logs.push((tp, Arc::new(log)));
            } else if dir.file_name().to_string_lossy().ends_with("checkpoints") {
                trace!("skip recovery file: {:?}", dir.path().to_string_lossy());
            } else {
                warn!("invalid log dir:{:?}", dir.path().to_string_lossy());
            }
        }
        Ok(logs)
    }

    pub fn load_queue_logs(
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

        let logs = self.do_load_queue_logs(index_file_max_size)?;
        info!(
            "load {} logs from dir:{} finished",
            logs.len(),
            self.queue_log_path
        );
        Ok(logs)
    }

    fn do_load_queue_logs(
        &self,
        index_file_max_size: u32,
    ) -> AppResult<Vec<(TopicPartition, Arc<QueueLog>)>> {
        // 加载检查点文件
        let recovery_checkpoints = self
            .queue_recovery_checkpoints
            .read_checkpoints(LogType::Queue)?;

        let mut logs = Vec::with_capacity(1010);
        let mut dir = std::fs::read_dir(&self.queue_log_path)
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
                let dir_name = dir.file_name().to_string_lossy().into_owned();
                let tp = TopicPartition::from_str(&dir_name, LogType::Queue)?;
                let recovery_offset = recovery_checkpoints.get(&tp).unwrap_or(&0).to_owned();
                let log = QueueLog::load_from(&tp, recovery_offset, index_file_max_size)?;
                trace!("found log:{:}", &tp.id());
                logs.push((tp, Arc::new(log)));
            } else {
                warn!("invalid queue log dir:{:?}", dir.path().to_string_lossy());
            }
        }
        Ok(logs)
    }

    pub fn get_or_create_journal_log(
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

                let journal_log = JournalLog::new(topic_partition)?;
                let log = Arc::new(journal_log);
                vacant.insert(log.clone());
                Ok(log)
            }
        }
    }
    pub fn get_or_create_queue_log(
        &self,
        topic_partition: &TopicPartition,
    ) -> AppResult<Arc<QueueLog>> {
        let log = self.queue_logs.entry(topic_partition.clone());

        match log {
            Entry::Occupied(occupied) => Ok(occupied.get().clone()),
            Entry::Vacant(vacant) => {
                let log = Arc::new(QueueLog::new(topic_partition)?);
                vacant.insert(log.clone());
                Ok(log)
            }
        }
    }
}
