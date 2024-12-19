//! Log management module for handling journal and queue logs.
//!
//! This module provides functionality for:
//! - Log segment management
//! - Checkpoint handling
//! - Index file management
//! - Log reading and writing operations

mod checkpoint;
mod file_writer;
mod index_file;
mod journal_log;
mod log_manager;
mod log_reader;
mod log_segment;
mod queue_log;
mod splitter;

// Re-exports
pub use checkpoint::CheckPointFile;
pub use journal_log::JournalLog;
pub use log_manager::LogManager;
pub use log_reader::seek;
pub use log_segment::PositionInfo;
pub use queue_log::QueueLog;

use file_writer::ActiveLogFileWriter;
use once_cell::sync::Lazy;

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

/// Global file writer instance
static ACTIVE_LOG_FILE_WRITER: Lazy<ActiveLogFileWriter> = Lazy::new(ActiveLogFileWriter::new);

/// Information about a log append operation
#[derive(Debug, Clone)]
pub struct LogAppendInfo {
    /// First offset in the append batch
    pub first_offset: i64,
    /// Last offset in the append batch
    pub last_offset: i64,
    /// Maximum timestamp in the batch
    pub _max_timestamp: i64,
    /// Offset of the record with maximum timestamp
    pub _offset_of_max_timestamp: i64,
    /// Number of records in the batch
    pub records_count: u32,
    /// Timestamp when the append occurred
    pub _log_append_time: i64,
}
