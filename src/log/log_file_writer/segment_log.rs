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

/// Represents a segment of a log file that handles both journal and queue writes
///
/// # Fields
///
/// * `path` - Path to the segment file
/// * `received_write` - Total amount of data received for writing
/// * `acc_buffer` - Accumulation buffer for write operations
/// * `file_write` - Amount of data successfully written to disk
#[derive(Debug)]
pub struct SegmentLog {
    path: PathBuf,
    received_write: AtomicU64,
    acc_buffer: WriteBuffer,
    file_write: AtomicU64,
}

impl SegmentLog {
    /// Creates a new SegmentLog instance
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

    /// Write journal log
    /// # Arguments
    ///
    /// * `request` - Journal write request containing:
    ///   * records - Message records
    ///   * journal_offset - Journal log offset
    ///   * queue_topic_partition - Queue topic partition info
    ///   * first_batch_queue_base_offset - First batch queue base offset
    ///   * last_batch_queue_base_offset - Last batch queue base offset
    ///   * records_count - Number of records
    ///
    /// # Returns
    ///
    /// Returns an IO result:
    /// * `Ok(())` - Write successful
    /// * `Err(io::Error)` - Write failed
    ///
    /// # Errors
    ///
    /// Returns error when file operations fail
    /// journal log format:
    /// batch_size + journal_offset + topic_partition_id_string_size + topic_partition _id_string + first_batch_queue_base_offset + last_batch_queue_base_offset + records_count + msg
    /// batch_size does not include the 4 bytes of itself
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

    /// Writes queue data to the segment
    ///
    /// # Arguments
    ///
    /// * `request` - Queue write request containing records to write
    ///
    /// # Returns
    ///
    /// Returns an IO Result:
    /// * `Ok(())` - Write successful
    /// * `Err(io::Error)` - Write failed
    ///
    /// # Errors
    ///
    /// Returns error when file operations fail
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

    /// Flushes buffered data to disk
    ///
    /// # Returns
    ///
    /// Returns an IO Result containing:
    /// * The total file size after flush
    ///
    /// # Errors
    ///
    /// Returns error when file operations fail
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

    /// Returns the total size of data received for writing
    ///
    /// # Returns
    ///
    /// The total size in bytes of all data received for writing
    pub fn file_size(&self) -> u64 {
        self.received_write.load(Ordering::Acquire)
    }

    /// Returns the size of data that has been written to disk
    ///
    /// # Returns
    ///
    /// The size in bytes of data that has been successfully written to disk
    pub fn readable_size(&self) -> u64 {
        self.file_write.load(Ordering::Acquire)
    }
}

/// Buffer for accumulating write operations before flushing to disk
///
/// # Fields
///
/// * `buffer` - The actual buffer storing data
/// * `last_flush` - Timestamp of the last flush operation
/// * `config` - Configuration for write operations including buffer capacity and flush interval
#[derive(Debug)]
struct WriteBuffer {
    buffer: Option<Vec<u8>>,
    last_flush: Instant,
    config: WriteConfig,
}

impl WriteBuffer {
    /// Creates a new WriteBuffer instance
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for the write buffer
    ///
    /// # Returns
    ///
    /// Returns a new WriteBuffer instance
    pub fn new(config: &WriteConfig) -> Self {
        let config_clone = config.clone();
        Self {
            buffer: Some(Vec::with_capacity(config.buffer_capacity)),
            last_flush: Instant::now(),
            config: config_clone,
        }
    }

    /// Attempts to write data to the buffer
    ///
    /// # Arguments
    ///
    /// * `data` - Data to write to the buffer
    ///
    /// # Returns
    ///
    /// Returns true if the buffer should be flushed, false otherwise
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

    /// Determines if the buffer should be flushed
    ///
    /// # Arguments
    ///
    /// * `incoming_size` - Size of incoming data
    ///
    /// # Returns
    ///
    /// Returns true if the buffer should be flushed based on capacity or time criteria
    fn should_flush(&self, incoming_size: usize) -> bool {
        self.buffer.as_ref().unwrap().len() + incoming_size >= self.config.buffer_capacity
            || self.last_flush.elapsed() >= self.config.flush_interval
    }
}
