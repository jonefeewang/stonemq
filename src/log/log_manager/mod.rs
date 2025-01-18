//! Log Manager Module
//!
//! This module provides centralized management of journal and queue logs in the message queue system.
//! It handles log initialization, recovery, checkpointing, and lifecycle management for both journal
//! and queue logs.
//!
//! # Architecture
//!
//! The log manager maintains two types of logs:
//! - Journal Logs: Store the original messages in the order they are received
//! - Queue Logs: Store messages organized by topics and partitions for consumption
//!
//! Each type of log has its own:
//! - Recovery checkpoint file to track progress
//! - Directory structure
//! - Index file size configuration
//!
//! # Components
//!
//! - `LogManager`: Central coordinator for all log operations
//! - `logmanager_load`: Handles log loading and recovery during startup
//! - `logmanager_tasks`: Implements background tasks like checkpointing
//!
//! # Checkpointing
//!
//! The system maintains three types of checkpoints:
//! - Journal Recovery Checkpoint: Tracks recovery progress of journal logs
//! - Queue Recovery Checkpoint: Tracks recovery progress of queue logs
//! - Split Checkpoint: Tracks the progress of journal to queue log conversion

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

/// Central manager for all log operations in the message queue system.
///
/// The LogManager coordinates operations between journal logs and queue logs,
/// manages their lifecycle, and ensures proper recovery and checkpointing.
///
/// # Fields
///
/// * `journal_logs` - Thread-safe map of journal logs indexed by topic partition
/// * `queue_logs` - Thread-safe map of queue logs indexed by topic partition
/// * `journal_recovery_checkpoints` - Checkpoint file for journal log recovery
/// * `queue_recovery_checkpoints` - Checkpoint file for queue log recovery
/// * `split_checkpoint` - Checkpoint file for journal to queue conversion
/// * `notify_shutdown` - Broadcast channel for shutdown notifications
/// * `_shutdown_complete_tx` - Channel to signal shutdown completion
/// * `journal_log_path` - Base directory path for journal logs
/// * `queue_log_path` - Base directory path for queue logs
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
    /// Creates a new LogManager instance.
    ///
    /// Initializes the log manager with the necessary checkpoint files and
    /// communication channels for shutdown coordination.
    ///
    /// # Arguments
    ///
    /// * `notify_shutdown` - Channel sender for shutdown notifications
    /// * `shutdown_complete_tx` - Channel sender to signal shutdown completion
    ///
    /// # Returns
    ///
    /// A new instance of LogManager
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

    /// Starts up the log manager and initializes all necessary components.
    ///
    /// This method:
    /// 1. Initializes the active segment writer
    /// 2. Loads existing journal logs
    /// 3. Loads existing queue logs
    /// 4. Recovers any necessary state from checkpoints
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - Success if startup completes, error otherwise
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

    /// Starts the background checkpoint task.
    ///
    /// Launches a background task that periodically:
    /// 1. Updates recovery checkpoints for both journal and queue logs
    /// 2. Ensures consistency between journal and queue logs
    /// 3. Handles cleanup of processed logs
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - Success if task starts, error otherwise
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
    /// Cleanup handler for LogManager.
    ///
    /// Ensures proper cleanup of resources when the LogManager is dropped.
    fn drop(&mut self) {
        debug!("log manager dropped");
    }
}
