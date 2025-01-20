//! Message Processing Implementation
//!
//! This module implements the core message processing functionality, including:
//! - Message batch handling
//! - Record format and serialization
//! - Topic and partition management
//! - Memory-based record storage
//!
//! # Architecture
//!
//! The message system is built around these key concepts:
//! - `BatchHeader`: Metadata for message batches
//! - `MemoryRecords`: In-memory representation of message records
//! - `RecordBatch`: Collection of related records
//! - `TopicPartition`: Logical division of message streams
//!
//! # Components
//!
//! - `batch_header`: Message batch metadata handling
//! - `constants`: System-wide constants and configurations
//! - `memory_records`: In-memory record management
//! - `record`: Individual message record implementation
//! - `record_batch`: Batch processing functionality
//! - `topic_partition`: Topic and partition management
//!
//! # Features
//!
//! - Efficient batch processing
//! - Memory-optimized record storage
//! - Flexible partition management
//! - Thread-safe operations

mod batch_header;
mod constants;
mod memory_records;
mod record;
mod record_batch;
mod record_batch_test;
mod topic_partition;

pub use memory_records::MemoryRecords;
pub use record_batch::RecordBatch;
pub use topic_partition::{JournalPartition, QueuePartition};
pub use topic_partition::{PartitionMsgData, TopicData, TopicPartition};

use crate::log::PositionInfo;

/// Information about fetched log records.
///
/// Contains the fetched records along with metadata about the log segment
/// they were retrieved from.
///
/// # Fields
///
/// * `records` - The fetched message records
/// * `log_start_offset` - First valid offset in the log
/// * `log_end_offset` - Last offset in the log
/// * `position_info` - Position information for the fetched records
#[derive(Debug)]
pub struct LogFetchInfo {
    pub records: MemoryRecords,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
    pub position_info: PositionInfo,
}
