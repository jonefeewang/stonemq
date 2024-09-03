pub use log_manager::LogManager;
pub use checkpoint::CheckPointFile;
pub use queue_log::QueueLog;
pub use journal_log::JournalLog;
mod checkpoint;
mod file_records;
mod index_file;
mod journal_log;
mod log_manager;
mod log_segment;
mod queue_log;

use crate::AppError::IllegalStateError;
use crate::{AppError, AppResult};
use log_segment::LogSegment;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use crate::message::{TopicPartition,MemoryRecords,LogAppendInfo};


pub trait Log: Debug {
    async fn append_records(
        &self,
        records: (TopicPartition, MemoryRecords),
    ) -> AppResult<LogAppendInfo>;
    fn load_segments(dir: &str, rt: &Runtime) -> AppResult<BTreeMap<u64, LogSegment>>;
    async fn new(
        dir: String,
        segments: BTreeMap<u64, LogSegment>,
        log_start_offset: u64,
        log_recovery_point: u64,
    ) -> AppResult<Self>
    where
        Self: Sized;
    async fn flush(&self) -> AppResult<()>;
    fn no_active_segment_error(&self, dir: String) -> AppError {
        IllegalStateError(Cow::Owned(format!("no active segment found log:{}", dir,)))
    }
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

const JOURNAL_CHECK_POINT_FILE_NAME: &str = ".journal_recovery_checkpoints";
