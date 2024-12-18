mod file_info;
mod file_request;
mod file_writer;

use crate::message::TopicPartition;
use async_channel::Sender;
use dashmap::DashMap;
use file_info::FileInfo;
pub use file_request::FlushRequest;
pub use file_request::JournalLogWriteOp;
pub use file_request::QueueLogWriteOp;
pub use file_request::LogWriteRequest;

use std::{path::PathBuf, sync::Arc};

#[derive(Debug)]
pub struct FileWriter {
    writers: Arc<DashMap<TopicPartition, FileInfo>>,
    base_dir: PathBuf,
    request_tx: Sender<LogWriteRequest>,
}
