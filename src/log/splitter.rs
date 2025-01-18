//! Log Splitter Implementation
//!
//! This module implements the log splitting functionality that converts journal logs
//! into queue logs. It provides mechanisms for reading from journal logs and writing
//! to corresponding queue logs while maintaining consistency and handling failures.
//!
//! # Architecture
//!
//! The splitter operates as a background task that:
//! - Reads records from journal logs
//! - Processes and converts records
//! - Writes records to appropriate queue logs
//! - Maintains progress tracking
//!
//! # Features
//!
//! - Asynchronous operation
//! - Graceful shutdown handling
//! - Error recovery
//! - Progress tracking
//! - Concurrent access to logs
//!
//! # Recovery
//!
//! The splitter implements recovery mechanisms for:
//! - Interrupted operations
//! - File system errors
//! - Segment transitions
//! - Process crashes

use crate::log::{JournalLog, LogType};
use crate::message::{MemoryRecords, TopicPartition};
use crate::{global_config, AppError, AppResult, Shutdown};
use bytes::{Buf, BytesMut};

use std::collections::BTreeMap;
use std::error::Error;
use std::fs::File;
use std::io::{self, ErrorKind, Read};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::time::Interval;
use tracing::{debug, error, instrument, trace};

use super::queue_log::QueueLog;
use super::JournalRecordsBatch;

/// Task responsible for splitting journal logs into queue logs.
///
/// The SplitterTask reads records from journal logs and writes them to
/// corresponding queue logs, maintaining consistency and handling failures.
///
/// # Fields
///
/// * `journal_log` - Source journal log to read from
/// * `queue_logs` - Destination queue logs to write to
/// * `topic_partition` - Topic partition being processed
/// * `read_wait_interval` - Interval for retry attempts
/// * `_shutdown_complete_tx` - Channel for shutdown coordination
#[derive(Debug)]
pub struct SplitterTask {
    journal_log: Arc<JournalLog>,
    queue_logs: BTreeMap<TopicPartition, Arc<QueueLog>>,
    topic_partition: TopicPartition,
    read_wait_interval: Interval,
    _shutdown_complete_tx: Sender<()>,
}

impl SplitterTask {
    /// Creates a new SplitterTask instance.
    ///
    /// # Arguments
    ///
    /// * `journal_log` - Source journal log
    /// * `queue_logs` - Map of destination queue logs
    /// * `topic_partition` - Topic partition to process
    /// * `read_wait_interval` - Interval for retry attempts
    /// * `_shutdown_complete_tx` - Shutdown coordination channel
    ///
    /// # Returns
    ///
    /// A new SplitterTask instance
    pub fn new(
        journal_log: Arc<JournalLog>,
        queue_logs: BTreeMap<TopicPartition, Arc<QueueLog>>,
        topic_partition: TopicPartition,
        read_wait_interval: Interval,
        _shutdown_complete_tx: Sender<()>,
    ) -> Self {
        SplitterTask {
            journal_log,
            queue_logs,
            topic_partition,
            read_wait_interval,
            _shutdown_complete_tx,
        }
    }

    /// Runs the splitter task, processing records until shutdown.
    ///
    /// This is the main processing loop that:
    /// - Reads records from journal log
    /// - Handles errors and retries
    /// - Processes records in batches
    /// - Updates progress tracking
    ///
    /// # Arguments
    ///
    /// * `shutdown` - Shutdown signal receiver
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - Success if shutdown is clean
    #[instrument(name = "splitter_run", skip_all, fields(target_offset))]
    pub async fn run(&mut self, mut shutdown: Shutdown) -> AppResult<()> {
        debug!(
            "splitter task for journal: {} and queues: {:?}",
            self.topic_partition.id(),
            self.queue_logs
                .keys()
                .map(|tp| tp.id())
                .collect::<Vec<String>>()
        );
        while !shutdown.is_shutdown() {
            let target_offset = self.journal_log.split_offset.load(Ordering::Acquire) + 1;
            debug!(
                "start loop and read target offset: {} / {}",
                target_offset, &self.topic_partition
            );

            match self.read_from(target_offset, &mut shutdown).await {
                // Normal read and processing, read until EOF of the segment, complete processing of this segment, return Ok(()), wait to switch to next segment
                Ok(()) => continue,
                Err(e) if self.is_retrievable_error(&e) => {
                    trace!(
                        "读取目标offset失败,可恢复中: {}/{}",
                        e,
                        &self.topic_partition
                    );
                    if shutdown.is_shutdown() {
                        return Ok(());
                    }
                    tokio::select! {
                        _ = self.read_wait_interval.tick() => {
                            trace!("等待继续读取...... / {}", &self.topic_partition);
                        },
                        _ = shutdown.recv() => {
                            return Ok(());
                        }
                    }
                    continue;
                }
                Err(e) => {
                    error!(
                        "读取目标offset失败,不可恢复: {}/{}",
                        e, &self.topic_partition
                    );
                    return Err(e);
                }
            }
        }
        debug!("splitter task exit");
        Ok(())
    }

    /// Determines if an error is recoverable.
    ///
    /// Analyzes error types to determine if retry is possible.
    ///
    /// # Arguments
    ///
    /// * `e` - Error to analyze
    ///
    /// # Returns
    ///
    /// `true` if error is recoverable
    fn is_retrievable_error(&self, e: &AppError) -> bool {
        trace!("is_retrievable_error: {:?}/{}", e, &self.topic_partition);
        e.source()
            .and_then(|e| e.downcast_ref::<std::io::Error>())
            .map_or(false, |std_err| {
                matches!(
                    std_err.kind(),
                    ErrorKind::NotFound | ErrorKind::UnexpectedEof
                )
            })
    }

    /// Reads records from a specific offset.
    ///
    /// Handles the complete read process including:
    /// - Finding the correct segment
    /// - Positioning at the right offset
    /// - Reading records
    /// - Processing batches
    ///
    /// # Arguments
    ///
    /// * `target_offset` - Offset to start reading from
    /// * `shutdown` - Shutdown signal receiver
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - Success if read completes
    async fn read_from(&mut self, target_offset: i64, shutdown: &mut Shutdown) -> AppResult<()> {
        // Simply seek the file pointer to the target offset and continue reading until the end of the segment (EOF). Once this segment is fully processed, return Ok(()); 
        // Any EOF errors encountered during reading are handled internally.
        // Function return values:
        // 1. Normal read and processing: Read until EOF of the segment, complete processing of this segment, return Ok(()), wait to switch to next segment
        // 2. get_relative_position_info returns not found, or seek_file returns NotFound or EOF error, File::open returns NotFound, return Err(e), these errors need retry downstream
        // 3. Other file errors: return Err(e), unrecoverable errors, downstream should explicitly throw and exit
        let refer_position_info = self.journal_log.get_relative_position_info(target_offset)?;

        let journal_topic_dir = PathBuf::from(global_config().log.journal_base_dir.clone())
            .join(self.topic_partition.id());
        let segment_path =
            journal_topic_dir.join(format!("{}.log", refer_position_info.base_offset));
        let journal_seg_file = std::fs::File::open(&segment_path)?;

        debug!(
            // Starting to read a segment
            "Starting to read segment {:?} ref: {:?}, target offset: {} / {}",
            segment_path, &refer_position_info, target_offset, &self.topic_partition
        );

        let (file, exact_position) = crate::log::seek_file(
            journal_seg_file,
            target_offset,
            refer_position_info,
            LogType::Journal,
        )
        .await?;

        trace!(
            // File internal read pointer position
            "File internal read pointer position: {}/{}",
            exact_position.position,
            &self.topic_partition
        );

        loop {
            if shutdown.is_shutdown() {
                return Ok(());
            }
            let file_clone = file.try_clone()?;
            let read_result = tokio::select! {
                read_result = self.read_batch(file_clone) => read_result,
                _ = shutdown.recv() => {
                    return Ok(());
                }
            };

            match read_result {
                Ok(journal_records_batch) => {
                    // Successfully read a batch from journal log, append to the corresponding queue log
                    let records_count = journal_records_batch.records_count;
                    let journal_offset = journal_records_batch.journal_offset;
                    self.queue_logs
                        .get(&journal_records_batch.queue_topic_partition)
                        .unwrap()
                        .append_records(journal_records_batch)
                        .await?;

                    self.journal_log
                        .split_offset
                        .store(journal_offset, Ordering::Release);
                    trace!(
                        // Process batch and update split offset
                        "Processed batch of {} records, updated split offset to {}/{}",
                        records_count,
                        self.journal_log.split_offset.load(Ordering::Acquire),
                        &self.topic_partition
                    );
                    continue;
                }
                Err(e) => {
                    // Read batch from journal log failed
                    if let Some(io_err) =
                        e.source().and_then(|e| e.downcast_ref::<std::io::Error>())
                    {
                        trace!(
                            // IO error occurred
                            "IO error occurred: {:?}/{}",
                            io_err,
                            &self.topic_partition
                        );
                        if io_err.kind() == ErrorKind::UnexpectedEof {
                            // When EOF is encountered, there are two possibilities:
                            // 1. Historical segment: already finished reading
                            // 2. Active segment: data temporarily unavailable (possibly still being written), truly reached end of active segment
                            if self.is_active_segment(exact_position.base_offset)? {
                                if shutdown.is_shutdown() {
                                    return Ok(());
                                }
                                trace!(
                                    // Current segment is active, waiting to retry
                                    "Current segment is active, waiting to retry / {}",
                                    &self.topic_partition
                                );
                                tokio::select! {
                                    _ = self.read_wait_interval.tick() => {},
                                    _ = shutdown.recv() => return Ok(()),
                                }
                                continue;
                            } else {
                                // If current segment is not active segment,
                                // return and switch to next segment
                                return Ok(());
                            }
                        }
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Checks if a segment is the active segment.
    ///
    /// # Arguments
    ///
    /// * `base_offset` - Base offset to check
    ///
    /// # Returns
    ///
    /// * `AppResult<bool>` - True if segment is active
    fn is_active_segment(&self, base_offset: i64) -> AppResult<bool> {
        let current_active_seg_offset = self.journal_log.current_active_seg_offset();
        Ok(base_offset == current_active_seg_offset)
    }

    /// Reads a batch of records from the journal.
    ///
    /// Handles the low-level reading of record batches including:
    /// - Reading batch metadata
    /// - Parsing records
    /// - Creating batch objects
    ///
    /// # Arguments
    ///
    /// * `file` - File to read from
    ///
    /// # Returns
    ///
    /// * `AppResult<JournalRecordsBatch>` - Batch of records
    async fn read_batch(&self, mut file: File) -> AppResult<JournalRecordsBatch> {
        // Read a batch from journal log
        let mut buf = tokio::task::spawn_blocking({
            move || -> io::Result<BytesMut> {
                let mut buffer = [0u8; 4];
                file.read_exact(&mut buffer)?;
                let batch_size = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
                let mut buf = BytesMut::zeroed(batch_size as usize);
                // EOF error may occur here and will be returned
                file.read_exact(&mut buf)?;
                Ok(buf)
            }
        })
        .await
        .map_err(|e| AppError::DetailedIoError(format!("Failed to read file: {}", e)))??;

        // Parse the buffer and create a JournalRecordsBatch
        let journal_offset = buf.get_i64();
        let tp_str_size = buf.get_u32();
        // Queue topic partition string
        let mut tp_str_bytes = vec![0; tp_str_size as usize];
        buf.copy_to_slice(&mut tp_str_bytes);
        let queue_topic_partition_str = String::from_utf8(tp_str_bytes).unwrap();
        let queue_topic_partition =
            TopicPartition::from_str(&queue_topic_partition_str, LogType::Queue)?;
        let first_batch_queue_base_offset = buf.get_i64();
        let last_batch_queue_base_offset = buf.get_i64();
        let records_count = buf.get_u32();
        let memory_records = MemoryRecords::new(buf);

        Ok(JournalRecordsBatch {
            journal_offset,
            queue_topic_partition,
            first_batch_queue_base_offset,
            last_batch_queue_base_offset,
            records_count,
            records: memory_records,
        })
    }
}

