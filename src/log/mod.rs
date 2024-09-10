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

use crate::message::{LogAppendInfo, MemoryRecords, TopicPartition};
use crate::AppError::IllegalStateError;
use crate::{AppError, AppResult};
use log_segment::LogSegment;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;


pub trait Log: Debug {
    async fn append_records(
        &self,
        records: (TopicPartition, MemoryRecords),
    ) -> AppResult<LogAppendInfo>;
    fn load_segments(topic_partition: &TopicPartition, next_offset: u64, rt: &Runtime) -> AppResult<BTreeMap<u64, LogSegment>>;
    async fn new(
        topic_partition: &TopicPartition,
        segments: BTreeMap<u64, LogSegment>,
        log_start_offset: u64,
        log_recovery_point: u64,
    ) -> AppResult<Self>
    where
        Self: Sized;
    async fn flush(&self, active_segment: &LogSegment) -> AppResult<()>;
    fn no_active_segment_error(&self, dir: String) -> AppError {
        IllegalStateError(Cow::Owned(format!("no active segment found log:{}", dir, )))
    }
}
pub(crate) enum LogType {
    JournalLog,
    QueueLog,
}

pub enum FileOp {
    AppendRecords(
        (
            TopicPartition,
            MemoryRecords,
            oneshot::Sender<AppResult<()>>,
        ),
    ),
    FetchRecords,
    Flush(oneshot::Sender<AppResult<(u64)>>),
}

const CHECK_POINT_FILE_NAME: &str = ".recovery_checkpoints";
