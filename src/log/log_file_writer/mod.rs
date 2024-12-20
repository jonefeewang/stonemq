//! Log file writer module for handling journal and queue logs.
//!
//! In async Rust, it is not possible to hold a `MutexGuard` across an `.await`. As a result,
//! the log segment is split into an index file and a log segment. Operations on the index file are
//! synchronous and delegated to the Log manager, while operations on the log segment are
//! asynchronous. All active log segment files are centrally managed through a pipeline, avoiding the
//! need for each log to initiate its own channel. This design eliminates the necessity of acquiring a
//! log segment lock before performing asynchronous file write or flush operations in Journal logs or
//! Queue logs. An async channel must be used here to prevent blocking the Tokio runtime.

mod active_log_file_writer;
mod log_request;
mod segment_log;

use crate::message::TopicPartition;
use crate::utils::MultipleChannelWorkerPool;
use dashmap::DashMap;

pub use log_request::FileWriteRequest;
pub use log_request::FlushRequest;
pub use log_request::QueueFileWriteReq;

use segment_log::SegmentLog;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender;

use std::sync::Arc;

#[derive(Debug)]
pub struct ActiveLogFileWriter {
    worker_pool: MultipleChannelWorkerPool<FileWriteRequest>,
    writers: Arc<DashMap<TopicPartition, SegmentLog>>,
}

use once_cell::sync::OnceCell;

pub static ACTIVE_LOG_FILE_WRITER: OnceCell<Arc<ActiveLogFileWriter>> = OnceCell::new();

impl ActiveLogFileWriter {
    pub fn global_init(
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: Sender<()>,
    ) -> &'static Arc<ActiveLogFileWriter> {
        ACTIVE_LOG_FILE_WRITER
            .get_or_init(|| Arc::new(Self::new(notify_shutdown, shutdown_complete_tx, None)))
    }
}

pub fn global_active_log_file_writer() -> &'static Arc<ActiveLogFileWriter> {
    ACTIVE_LOG_FILE_WRITER.get().unwrap()
}
