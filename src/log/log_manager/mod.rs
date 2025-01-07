use crate::log::{
    CheckPointFile, JournalLog, QueueLog, RECOVERY_POINT_FILE_NAME, SPLIT_POINT_FILE_NAME,
};

mod logmanager_load;
mod logmanager_tasks;

use dashmap::DashMap;
use std::time::Duration;

use crate::message::TopicPartition;
use crate::{global_config, AppResult, Shutdown};
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info, trace};

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

    pub fn startup(&mut self) -> AppResult<()> {
        info!("log manager startup ...");
        
        Self::init_active_segment_writer(self.notify_shutdown.clone());

        let log_config = &global_config().log;
        let journal_index_file_size = log_config.journal_index_file_size as u32;
        let queue_index_file_size = log_config.queue_index_file_size as u32;

        let journal_logs = self.load_journal_logs(journal_index_file_size)?;
        self.journal_logs.extend(journal_logs);

        let queue_logs = self.load_queue_logs(queue_index_file_size)?;
        self.queue_logs.extend(queue_logs);

        Ok(())
    }

    pub async fn start_checkpoint_task(self: Arc<Self>) -> AppResult<()> {
        let recovery_check_interval = global_config().log.recovery_checkpoint_interval;
        let interval = tokio::time::interval(Duration::from_secs(recovery_check_interval));
        let shutdown = Shutdown::new(self.notify_shutdown.subscribe());
        tokio::spawn(async move {
            let result = self.recovery_checkpoint_task(interval, shutdown).await;
            match result {
                Ok(_) => {
                    trace!("journal log recovery checkpoint task shutdown");
                }
                Err(error) => {
                    error!("recovery checkpoint task error:{:?}", error);
                }
            }
        });
        Ok(())
    }
}

impl Drop for LogManager {
    fn drop(&mut self) {
        debug!("log manager dropped");
    }
}
