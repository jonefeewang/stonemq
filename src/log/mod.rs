pub use checkpoint::CheckPointFile;
pub use index_file::IndexFile;
pub use journal_log::JournalLog;
pub use log_manager::LogManager;
pub use log_segment::PositionInfo;
pub use queue::QueueLog;
mod checkpoint;
mod file_records;
mod index_file;
mod journal_log;
mod log_manager;
mod log_segment;
mod queue;
mod splitter;

use crate::message::{MemoryRecords, TopicPartition};
use crate::AppResult;
use tokio::sync::oneshot;

pub use log_segment::NO_POSITION_INFO;

/// Calculates the overhead for a journal log record.
///
/// # Arguments
///
/// * `topic_partition` - The topic partition.
///
/// # Returns
///
/// Returns the calculated overhead as a u32.
pub fn calculate_journal_log_overhead(topic_partition: &TopicPartition) -> u32 {
    //offset + tpstr size + tpstr + i64(first_batch_queue_base_offset)+i64(last_batch_queue_base_offset)+u32(records_count)
    8 + topic_partition.protocol_size() + 8 + 8 + 4
}

pub enum LogType {
    Journal,
    Queue,
}
pub enum FileOp {
    AppendJournal(
        (
            i64, // journal offset
            TopicPartition,
            i64, // first batch queue base offset
            i64, // last batch queue base offset
            u32, // records count
            MemoryRecords,
            oneshot::Sender<AppResult<()>>,
        ),
    ),
    AppendQueue(
        (
            i64, // journal offset
            TopicPartition,
            i64, // first batch queue base offset
            i64, // last batch queue base offset
            u32, // records count
            MemoryRecords,
            oneshot::Sender<AppResult<()>>,
        ),
    ),
    Flush(oneshot::Sender<AppResult<u64>>),
}

const RECOVERY_POINT_FILE_NAME: &str = ".recovery_checkpoints";
const SPLIT_POINT_FILE_NAME: &str = ".split_checkpoints";
const NEXT_OFFSET_CHECKPOINT_FILE_NAME: &str = ".next_offset_checkpoints";

#[derive(Debug, Clone)]
pub struct LogAppendInfo {
    pub first_offset: i64,
    pub last_offset: i64,
    pub max_timestamp: i64,
    pub offset_of_max_timestamp: i64,
    pub log_append_time: i64,
    pub records_count: u32,
}
