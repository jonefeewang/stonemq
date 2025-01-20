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

//! Log Write Request Types
//!
//! This module defines the various types of requests that can be made to the log file writer system.
//! It includes structures for journal writes, queue writes, and flush operations.

use tokio::sync::oneshot;

use crate::{message::TopicPartition, AppResult, MemoryRecords};

/// Request to write records to a journal log file.
///
/// This structure contains all necessary information to append records to a journal,
/// including both journal and queue metadata for proper record tracking.
#[derive(Debug)]
pub struct JournalFileWriteReq {
    /// Offset in the journal where these records will be written
    pub journal_offset: i64,
    /// Topic partition information for the journal
    pub topic_partition: TopicPartition,
    /// Associated queue topic partition information
    pub queue_topic_partition: TopicPartition,
    /// Base offset of the first batch in the queue
    pub first_batch_queue_base_offset: i64,
    /// Base offset of the last batch in the queue
    pub last_batch_queue_base_offset: i64,
    /// Total number of records in this write request
    pub records_count: u32,
    /// The actual records to be written
    pub records: MemoryRecords,
}

/// Request to write records to a queue log file.
///
/// Contains the necessary information to append records to a queue,
/// including the topic partition and the records themselves.
#[derive(Debug)]
pub struct QueueFileWriteReq {
    /// Topic partition information for the queue
    pub topic_partition: TopicPartition,
    /// The records to be written to the queue
    pub records: MemoryRecords,
}

/// Request to flush a log file to disk.
///
/// Used to ensure that all written data is persisted to disk
/// for a specific topic partition.
#[derive(Debug)]
pub struct FlushRequest {
    /// Topic partition whose log file needs to be flushed
    pub topic_partition: TopicPartition,
}

/// Enum representing different types of file write requests.
///
/// This enum is used by the worker pool to handle different types
/// of write operations, each with their own response channel for
/// handling the operation result.
#[derive(Debug)]
pub enum FileWriteRequest {
    /// Request to append records to a journal log
    AppendJournal {
        /// The journal write request details
        request: JournalFileWriteReq,
        /// Channel for sending the operation result
        reply: oneshot::Sender<AppResult<()>>,
    },
    /// Request to append records to a queue log
    AppendQueue {
        /// The queue write request details
        request: QueueFileWriteReq,
        /// Channel for sending the operation result
        reply: oneshot::Sender<AppResult<()>>,
    },
    /// Request to flush a log file to disk
    Flush {
        /// The flush request details
        request: FlushRequest,
        /// Channel for sending the operation result (returns the number of bytes flushed)
        reply: oneshot::Sender<AppResult<u64>>,
    },
}
