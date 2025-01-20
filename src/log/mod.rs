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

//! Log management module for handling journal and queue logs.
//!
//! This module provides functionality for:
//! - Log segment management
//! - Checkpoint handling
//! - Index file management
//! - Log reading and writing operations

mod checkpoint;
mod index_file;
mod journal_log;
mod log_file_writer;
mod log_manager;
mod log_reader;
mod queue_log;
mod segment_index;
mod splitter;

// Re-exports
pub use checkpoint::CheckPointFile;
pub use journal_log::JournalLog;
pub use log_manager::LogManager;
pub use log_reader::seek_file;
pub use queue_log::QueueLog;
pub use segment_index::PositionInfo;

pub use log_file_writer::get_active_segment_writer;
pub use log_file_writer::init_active_segment_writer;
pub use log_file_writer::WriteConfig;

use crate::message::TopicPartition;
use crate::MemoryRecords;

/// Represents a position in the log with no valid information
pub const NO_POSITION_INFO: PositionInfo = PositionInfo {
    base_offset: 0,
    offset: 0,
    position: 0,
};

/// Default timestamp for log append operations
pub const DEFAULT_LOG_APPEND_TIME: i64 = -1;

// File name constants
const RECOVERY_POINT_FILE_NAME: &str = ".recovery_checkpoints";
const SPLIT_POINT_FILE_NAME: &str = ".split_checkpoints";
const NEXT_OFFSET_CHECKPOINT_FILE_NAME: &str = ".next_offset_checkpoints";
const INDEX_FILE_SUFFIX: &str = "index";
const LOG_FILE_SUFFIX: &str = "log";

/// Types of logs supported by the system
#[derive(Debug, Clone, Copy, PartialEq, Ord, PartialOrd, Eq, Hash)]
pub enum LogType {
    /// Journal log type for storing raw messages
    Journal,
    /// Queue log type for storing processed messages
    Queue,
}
/// segment file type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentFileType {
    Index,
    Log,
    TimeIndex,
    Unknown,
}

/// Information about a log append operation
#[derive(Debug, Clone)]
pub struct LogAppendInfo {
    /// First offset in the append batch
    pub first_offset: i64,
    /// Last offset in the append batch
    pub _last_offset: i64,
    /// Maximum timestamp in the batch
    pub _max_timestamp: i64,
    /// Offset of the record with maximum timestamp
    pub _offset_of_max_timestamp: i64,
    /// Number of records in the batch
    pub _records_count: u32,
    /// Timestamp when the append occurred
    pub _log_append_time: i64,
}
/// journal records batch
#[derive(Debug)]
pub struct JournalRecordsBatch {
    pub journal_offset: i64,
    pub queue_topic_partition: TopicPartition,
    pub first_batch_queue_base_offset: i64,
    pub last_batch_queue_base_offset: i64,
    pub records_count: u32,
    pub records: MemoryRecords,
}
