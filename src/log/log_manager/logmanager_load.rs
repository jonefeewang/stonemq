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

//! Log Manager Loading Implementation
//!
//! This module implements the log loading and initialization functionality for the LogManager.
//! It handles loading both journal and queue logs from disk, including recovery from checkpoints
//! and creation of new logs when necessary.
//!
//! # Loading Process
//!
//! The loading process involves:
//! 1. Reading checkpoint files to determine recovery points
//! 2. Scanning log directories for existing logs
//! 3. Initializing log structures with appropriate recovery offsets
//! 4. Creating new logs when requested but not found
//!
//! # Error Handling
//!
//! The module provides detailed error handling for various failure scenarios:
//! - Missing directories
//! - Invalid log formats
//! - Corrupted checkpoint files
//! - I/O errors during loading

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
    /// Loads all journal logs from the configured journal log directory.
    ///
    /// This method scans the journal log directory and loads all valid journal logs,
    /// applying recovery checkpoints and split points as necessary.
    ///
    /// # Arguments
    ///
    /// * `index_file_max_size` - Maximum size for index files in bytes
    ///
    /// # Returns
    ///
    /// * `AppResult<Vec<(TopicPartition, Arc<JournalLog>)>>` - List of loaded journal logs
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Journal log directory doesn't exist
    /// - Directory scanning fails
    /// - Log loading fails
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

    /// Internal implementation for loading journal logs.
    ///
    /// Handles the actual loading process including:
    /// 1. Reading recovery and split checkpoints
    /// 2. Scanning directory for log files
    /// 3. Initializing journal logs with appropriate offsets
    ///
    /// # Arguments
    ///
    /// * `index_file_max_size` - Maximum size for index files
    ///
    /// # Returns
    ///
    /// * `AppResult<Vec<(TopicPartition, Arc<JournalLog>)>>` - List of loaded logs
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

    /// Loads all queue logs from the configured queue log directory.
    ///
    /// Similar to journal log loading, but specifically for queue logs.
    /// Applies recovery checkpoints during the loading process.
    ///
    /// # Arguments
    ///
    /// * `index_file_max_size` - Maximum size for index files in bytes
    ///
    /// # Returns
    ///
    /// * `AppResult<Vec<(TopicPartition, Arc<QueueLog>)>>` - List of loaded queue logs
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Queue log directory doesn't exist
    /// - Directory scanning fails
    /// - Log loading fails
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

    /// Internal implementation for loading queue logs.
    ///
    /// Handles the actual loading process including:
    /// 1. Reading recovery checkpoints
    /// 2. Scanning directory for log files
    /// 3. Initializing queue logs with appropriate offsets
    ///
    /// # Arguments
    ///
    /// * `index_file_max_size` - Maximum size for index files
    ///
    /// # Returns
    ///
    /// * `AppResult<Vec<(TopicPartition, Arc<QueueLog>)>>` - List of loaded logs
    fn do_load_queue_logs(
        &self,
        index_file_max_size: u32,
    ) -> AppResult<Vec<(TopicPartition, Arc<QueueLog>)>> {
        // Load checkpoint file
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

    /// Gets an existing journal log or creates a new one for the specified topic partition.
    ///
    /// This method provides thread-safe access to journal logs, creating new ones
    /// when necessary and ensuring only one instance exists per topic partition.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition to get or create a log for
    ///
    /// # Returns
    ///
    /// * `AppResult<Arc<JournalLog>>` - The journal log instance
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

    /// Gets an existing queue log or creates a new one for the specified topic partition.
    ///
    /// Similar to journal log creation, but for queue logs. Ensures thread-safe
    /// access and singleton instances per topic partition.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition to get or create a log for
    ///
    /// # Returns
    ///
    /// * `AppResult<Arc<QueueLog>>` - The queue log instance
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
