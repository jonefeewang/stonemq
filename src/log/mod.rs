pub use checkpoint::CheckPointFile;
pub use journal_log::JournalLog;
pub use log_manager::LogManager;
pub use queue_log::QueueLog;
mod checkpoint;
mod file_records;
mod index_file;
mod journal_log;
mod log_manager;
mod log_segment;
mod queue_log;
mod splitter;

use crate::message::{LogAppendInfo, MemoryRecords, TopicPartition};
use crate::AppError::IllegalStateError;
use crate::{AppError, AppResult};
use std::borrow::Cow;
use std::fmt::Debug;
use tokio::sync::oneshot;

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
    //  offset + tpstr size + tpstr
    8 + topic_partition.protocol_size()
}

pub trait Log: Debug {
    async fn append_records(
        &self,
        records: (TopicPartition, i64, MemoryRecords),
    ) -> AppResult<LogAppendInfo>;
    fn no_active_segment_error(&self, topic_partition: &TopicPartition) -> AppError {
        IllegalStateError(Cow::Owned(format!(
            "no active segment found log:{}",
            topic_partition,
        )))
    }
}
enum LogType {
    Journal,
    Queue,
}
pub enum FileOp {
    AppendJournal(
        (
            i64,
            TopicPartition,
            MemoryRecords,
            oneshot::Sender<AppResult<()>>,
        ),
    ),
    AppendQueue(
        (
            i64,
            TopicPartition,
            MemoryRecords,
            oneshot::Sender<AppResult<()>>,
        ),
    ),
    Flush(oneshot::Sender<AppResult<u64>>),
}

const RECOVERY_POINT_FILE_NAME: &str = ".recovery_checkpoints";
const SPLIT_POINT_FILE_NAME: &str = ".split_checkpoints";
const NEXT_OFFSET_CHECKPOINT_FILE_NAME: &str = ".next_offset_checkpoints";
