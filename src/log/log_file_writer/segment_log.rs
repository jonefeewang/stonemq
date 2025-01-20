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

//! Segment Log Implementation
//!
//! This module implements the core functionality for managing individual log segments.
//! A segment log is a portion of a larger log file that handles both journal and queue writes.
//! It provides efficient buffering and writing mechanisms to optimize I/O operations.
//!
//! # Design
//!
//! The segment log implementation uses a combination of:
//! - Buffered writes to optimize I/O performance
//! - Atomic counters to track write progress
//! - Async I/O operations for actual disk writes
//! - Separate tracking for received vs. written data
//!
//! # Components
//!
//! - `SegmentLog`: Main struct managing a single log segment
//! - `WriteBuffer`: Internal buffer managing write operations
//! - Atomic counters for thread-safe size tracking
//!
//! # Write Process
//!
//! 1. Data is received and tracked via `received_write`
//! 2. Data is accumulated in `WriteBuffer`
//! 3. When buffer is full or flush is triggered, data is written to disk
//! 4. Successfully written data is tracked via `file_write`

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Buf;
use tracing::debug;

use crate::log::{JournalLog, LOG_FILE_SUFFIX};
use crate::message::TopicPartition;

use super::log_request::{JournalFileWriteReq, QueueFileWriteReq};
use super::WriteConfig;

/// Represents a segment of a log file that handles both journal and queue writes.
///
/// A SegmentLog manages a single segment file, providing buffered write operations
/// and atomic tracking of write progress. It supports both journal and queue
/// write operations with different formats and requirements.
///
/// # Fields
///
/// * `path` - Path to the segment file on disk
/// * `received_write` - Atomic counter tracking total amount of data received for writing
/// * `acc_buffer` - Accumulation buffer for optimizing write operations
/// * `file_write` - Atomic counter tracking amount of data successfully written to disk
///
/// # Thread Safety
///
/// The struct uses atomic counters to ensure thread-safe tracking of write progress,
/// while the actual write operations are serialized through the async interface.
#[derive(Debug)]
pub struct SegmentLog {
    path: PathBuf,
    received_write: AtomicU64,
    acc_buffer: WriteBuffer,
    file_write: AtomicU64,
}

impl SegmentLog {
    /// Creates a new SegmentLog instance.
    ///
    /// Initializes a new segment log with the given parameters and creates
    /// the underlying file if it doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `base_offset` - Base offset for the segment
    /// * `topic_partition` - Topic partition information
    /// * `write_config` - Configuration for write operations
    ///
    /// # Returns
    ///
    /// Returns a new SegmentLog instance initialized with the given parameters
    pub fn new(
        base_offset: i64,
        topic_partition: &TopicPartition,
        write_config: &WriteConfig,
    ) -> Self {
        let path = format!(
            "{}/{}.{}",
            &topic_partition.partition_dir(),
            base_offset,
            LOG_FILE_SUFFIX
        );
        let file_size = Self::check_or_create_file(&path).unwrap();

        Self {
            path: path.into(),
            received_write: AtomicU64::new(file_size),
            acc_buffer: WriteBuffer::new(write_config),
            file_write: AtomicU64::new(file_size),
        }
    }

    /// Checks if a file exists at the given path and creates it if it doesn't
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file
    ///
    /// # Returns
    ///
    /// Returns an IO Result containing:
    /// * File size if file exists
    /// * 0 if file was created
    ///
    /// # Errors
    ///
    /// Returns error if file operations fail
    fn check_or_create_file(path: &str) -> io::Result<u64> {
        if Path::new(path).exists() {
            let metadata = std::fs::metadata(path)?;
            let file_size = metadata.len();
            Ok(file_size)
        } else {
            std::fs::File::create(path)?;
            Ok(0)
        }
    }

    /// Writes journal records to the segment.
    ///
    /// Handles the complex format of journal log entries, including metadata
    /// and actual message content.
    ///
    /// # Format
    ///
    /// Journal log entry format:
    /// ```text
    /// [batch_size(4)][journal_offset(8)][topic_partition_id_size(4)][topic_partition_id(var)]
    /// [first_queue_offset(8)][last_queue_offset(8)][records_count(4)][message_data(var)]
    /// ```
    /// Note: batch_size does not include its own 4 bytes
    ///
    /// # Arguments
    ///
    /// * `request` - Journal write request containing all necessary metadata and records
    ///
    /// # Returns
    ///
    /// Returns an IO Result indicating success or failure of the write operation
    pub async fn write_journal(&mut self, request: JournalFileWriteReq) -> io::Result<()> {
        let msg = request.records.buffer.unwrap();
        let total_size = JournalLog::calculate_journal_log_overhead(&request.topic_partition)
            + msg.remaining() as u32;

        // data + self length
        let total_write = total_size + 4;

        // prepare write data
        let mut buffer = Vec::with_capacity(total_write as usize);
        buffer.extend_from_slice(&total_size.to_be_bytes());
        buffer.extend_from_slice(&request.journal_offset.to_be_bytes());

        let tp_id = request.queue_topic_partition.id().to_string();
        let tp_id_bytes = tp_id.as_bytes();
        buffer.extend_from_slice(&(tp_id_bytes.len() as u32).to_be_bytes());
        buffer.extend_from_slice(tp_id_bytes);

        buffer.extend_from_slice(&request.first_batch_queue_base_offset.to_be_bytes());
        buffer.extend_from_slice(&request.last_batch_queue_base_offset.to_be_bytes());
        buffer.extend_from_slice(&request.records_count.to_be_bytes());
        buffer.extend_from_slice(msg.as_ref());

        // try write, if return Some then need flush
        if self.acc_buffer.try_write(&buffer) {
            // async execute flush
            let path = self.path.clone();
            let mut acc_buffer = self.acc_buffer.buffer.take().unwrap();
            let single_file_write = acc_buffer.len() + buffer.len();
            let acc_buffer = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)?;
                file.write_all(&acc_buffer)?;
                file.write_all(&buffer)?;
                acc_buffer.clear();
                Ok(acc_buffer)
            })
            .await??;
            self.acc_buffer.buffer = Some(acc_buffer);
            self.file_write
                .fetch_add(single_file_write as u64, Ordering::Release);
        }
        self.received_write
            .fetch_add(total_write as u64, Ordering::Release);
        Ok(())
    }

    /// Writes queue records to the segment.
    ///
    /// Simpler than journal writes as it only needs to append the raw message data.
    ///
    /// # Arguments
    ///
    /// * `request` - Queue write request containing records to write
    ///
    /// # Returns
    ///
    /// Returns an IO Result indicating success or failure of the write operation
    pub async fn write_queue(&mut self, request: QueueFileWriteReq) -> io::Result<()> {
        let msg = request.records.buffer.unwrap();
        let total_write = msg.remaining();

        if self.acc_buffer.try_write(msg.as_ref()) {
            let path = self.path.clone();
            let mut acc_buffer = self.acc_buffer.buffer.take().unwrap();
            let single_file_write = acc_buffer.len() + msg.len();
            let acc_buffer = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)?;
                file.write_all(&acc_buffer)?;
                file.write_all(msg.as_ref())?;
                acc_buffer.clear();
                Ok(acc_buffer)
            })
            .await??;
            self.acc_buffer.buffer = Some(acc_buffer);
            self.file_write
                .fetch_add(single_file_write as u64, Ordering::Release);
        }

        self.received_write
            .fetch_add(total_write as u64, Ordering::Release);
        Ok(())
    }

    /// Flushes all buffered data to disk.
    ///
    /// Forces any buffered data to be written to the underlying file and
    /// updates the file_write counter to match received_write.
    ///
    /// # Returns
    ///
    /// Returns an IO Result containing the total file size after flush
    pub async fn flush(&mut self) -> io::Result<u64> {
        let path = self.path.clone();

        let mut acc_buffer = self.acc_buffer.buffer.take().unwrap();
        let acc_buffer = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;

            file.write_all(&acc_buffer)?;
            file.sync_all()?;
            acc_buffer.clear();
            Ok(acc_buffer)
        })
        .await??;
        self.acc_buffer.buffer = Some(acc_buffer);

        let true_file_size = self.received_write.load(Ordering::Acquire);
        // update readable size
        self.file_write.store(true_file_size, Ordering::Release);

        Ok(true_file_size)
    }

    /// Returns the total size of data received for writing.
    ///
    /// This includes both data that has been written to disk and
    /// data that is still buffered.
    ///
    /// # Returns
    ///
    /// The total size in bytes of all data received for writing
    pub fn file_size(&self) -> u64 {
        self.received_write.load(Ordering::Acquire)
    }

    /// Returns the size of data that has been written to disk.
    ///
    /// This represents the amount of data that has been successfully
    /// flushed to the underlying file.
    ///
    /// # Returns
    ///
    /// The size in bytes of data written to disk
    pub fn readable_size(&self) -> u64 {
        self.file_write.load(Ordering::Acquire)
    }
}

/// Internal buffer for managing write operations.
///
/// Provides buffering functionality with configurable capacity and flush intervals
/// to optimize I/O performance.
///
/// # Fields
///
/// * `buffer` - Optional vector storing the actual data
/// * `last_flush` - Timestamp of the last flush operation
/// * `config` - Configuration controlling buffer behavior
#[derive(Debug)]
struct WriteBuffer {
    buffer: Option<Vec<u8>>,
    last_flush: Instant,
    config: WriteConfig,
}

impl WriteBuffer {
    /// Creates a new write buffer with the specified configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration controlling buffer capacity and flush intervals
    ///
    /// # Returns
    ///
    /// A new WriteBuffer instance initialized with the given configuration
    pub fn new(config: &WriteConfig) -> Self {
        let config_clone = config.clone();
        Self {
            buffer: Some(Vec::with_capacity(config.buffer_capacity)),
            last_flush: Instant::now(),
            config: config_clone,
        }
    }

    /// Attempts to write data to the buffer.
    ///
    /// Checks if the buffer should be flushed based on size and time criteria,
    /// and returns true if a flush is needed.
    ///
    /// # Arguments
    ///
    /// * `data` - The data to write to the buffer
    ///
    /// # Returns
    ///
    /// * `true` if the buffer should be flushed
    /// * `false` if the data was buffered without requiring a flush
    pub fn try_write(&mut self, data: &[u8]) -> bool {
        if self.should_flush(data.len()) {
            self.last_flush = Instant::now();
            debug!("flush buffer");
            true
        } else {
            debug!("try write buffer");
            self.buffer.as_mut().unwrap().extend_from_slice(data);
            false
        }
    }

    /// Determines if the buffer should be flushed.
    ///
    /// Checks both size and time-based criteria for flushing:
    /// - Buffer capacity threshold
    /// - Time since last flush
    ///
    /// # Arguments
    ///
    /// * `incoming_size` - Size of incoming data to consider
    ///
    /// # Returns
    ///
    /// `true` if the buffer should be flushed, `false` otherwise
    fn should_flush(&self, incoming_size: usize) -> bool {
        self.buffer.as_ref().unwrap().len() + incoming_size >= self.config.buffer_capacity
            || self.last_flush.elapsed() >= self.config.flush_interval
    }
}
