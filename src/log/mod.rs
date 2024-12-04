mod checkpoint;
mod file_records;
mod index_file;
mod journal_log;
mod log_manager;
mod log_segment;
mod queue_log;
mod splitter;

pub use checkpoint::CheckPointFile;
pub use index_file::IndexFile;
pub use journal_log::JournalLog;
pub use log_manager::LogManager;
pub use log_segment::PositionInfo;
pub use queue_log::QueueLog;

use crate::message::{MemoryRecords, TopicPartition};
use crate::AppResult;
use tokio::sync::oneshot;

pub const NO_POSITION_INFO: PositionInfo = PositionInfo {
    base_offset: 0,
    offset: 0,
    position: 0,
};

pub const DEFAULT_LOG_APPEND_TIME: i64 = -1;

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
    pub _max_timestamp: i64,
    pub _offset_of_max_timestamp: i64,
    pub _log_append_time: i64,
    pub records_count: u32,
}
