mod file_info;
mod file_request;
mod active_log_file_writer;

use crate::message::TopicPartition;
use async_channel::Sender;
use dashmap::DashMap;
use file_info::FileInfo;
pub use file_request::FlushRequest;
pub use file_request::JournalFileWriteReq;
pub use file_request::LogWriteRequest;
pub use file_request::QueueFileWriteReq;

use std::sync::Arc;

#[derive(Debug)]
pub struct ActiveLogFileWriter {
    writers: Arc<DashMap<TopicPartition, FileInfo>>,
    request_tx: Sender<LogWriteRequest>,
}
