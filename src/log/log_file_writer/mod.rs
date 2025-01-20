// Copyright 2025 jonefeewang@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Log File Writer Module
//!
//! This module provides a high-performance, asynchronous log file writing system for handling both journal
//! and queue logs in a message queue system. It implements a specialized architecture to handle the
//! challenges of async Rust and file I/O operations efficiently.
//!
//! # Architecture
//!
//! The module uses a worker pool pattern to manage active log segments, with the following key components:
//!
//! * `ActiveSegmentWriter`: Central manager for all active log segments, using a worker pool pattern
//! * `SegmentLog`: Handles individual log segment operations
//! * `FileWriteRequest`: Represents different types of write operations (journal, queue, flush)
//!
//! # Design Considerations
//!
//! In async Rust, it is not possible to hold a `MutexGuard` across an `.await`. As a result,
//! the log segment is split into an index file and a log segment. Operations on the index file are
//! synchronous and delegated to the Log, while operations on the log segment are
//! asynchronous. All active log segment files are centrally managed through a worker pool, avoiding the
//! need for each log to initiate its own channel. This design eliminates the necessity of acquiring a
//! log segment lock before performing asynchronous file write or flush operations in Journal logs or
//! Queue logs. An async channel must be used here to prevent blocking the Tokio runtime.
//!
//! # Components
//!
//! * `active_segment_writer`: Manages active segment writers for different topic partitions
//! * `log_request`: Defines various types of log write requests
//! * `segment_log`: Implements the core log segment functionality
//!
//! # Global Instance
//!
//! The module provides a global instance of `ActiveSegmentWriter` through `ACTIVE_SEGMENT_WRITER`,
//! which can be initialized once and accessed throughout the application.

mod active_segment_writer;
mod log_request;
mod segment_log;

use crate::message::TopicPartition;
use crate::utils::MultipleChannelWorkerPool;
use dashmap::DashMap;

pub use log_request::FileWriteRequest;
pub use log_request::FlushRequest;
pub use log_request::JournalFileWriteReq;
pub use log_request::QueueFileWriteReq;

use segment_log::SegmentLog;

use std::sync::Arc;
use std::time::Duration;

#[derive(Debug)]
pub struct ActiveSegmentWriter {
    worker_pool: MultipleChannelWorkerPool<FileWriteRequest>,
    writers: Arc<DashMap<TopicPartition, SegmentLog>>,
    write_config: WriteConfig,
}

#[derive(Debug, Clone)]
pub struct WriteConfig {
    pub buffer_capacity: usize,
    pub flush_interval: Duration,
}

impl Default for WriteConfig {
    fn default() -> Self {
        Self {
            buffer_capacity: 1024 * 1024,
            flush_interval: Duration::from_millis(500),
        }
    }
}

use std::sync::OnceLock;

pub static ACTIVE_SEGMENT_WRITER: OnceLock<Arc<ActiveSegmentWriter>> = OnceLock::new();

pub fn init_active_segment_writer(
    notify_shutdown: tokio::sync::broadcast::Sender<()>,
    worker_pool_config: Option<crate::utils::WorkerPoolConfig>,
    write_config: Option<WriteConfig>,
) {
    let writer = Arc::new(ActiveSegmentWriter::new(
        notify_shutdown,
        worker_pool_config,
        write_config,
    ));
    ACTIVE_SEGMENT_WRITER
        .set(writer)
        .expect("ActiveSegmentWriter already initialized");
}

pub fn get_active_segment_writer() -> &'static Arc<ActiveSegmentWriter> {
    ACTIVE_SEGMENT_WRITER
        .get()
        .expect("ActiveSegmentWriter not initialized")
}
