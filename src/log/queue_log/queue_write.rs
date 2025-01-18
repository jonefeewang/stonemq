//! Queue Log Writing Implementation
//!
//! This module implements the writing functionality for queue logs. It handles
//! appending records to segments, segment rolling, and maintaining write state.
//!
//! # Write Process
//!
//! The writing process involves several steps:
//! 1. Checking if a new segment is needed
//! 2. Rolling to a new segment if necessary
//! 3. Updating segment metadata
//! 4. Writing records to disk
//! 5. Updating offsets and recovery points
//!
//! # Segment Management
//!
//! The module handles segment lifecycle:
//! - Creating new segments when size limits are reached
//! - Converting active segments to read-only
//! - Maintaining segment metadata and indexes
//!
//! # Thread Safety
//!
//! Write operations are protected through:
//! - Atomic operations for offset updates
//! - Write locks for segment transitions
//! - Coordinated access to active segments

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::log::log_file_writer::{FlushRequest, QueueFileWriteReq};
use crate::log::segment_index::ActiveSegmentIndex;
use crate::log::{
    get_active_segment_writer, JournalRecordsBatch, LogAppendInfo, LogType, DEFAULT_LOG_APPEND_TIME,
};
use crate::message::MemoryRecords;
use crate::{global_config, AppResult};
use tracing::trace;

use super::QueueLog;

impl QueueLog {
    /// Appends records to the queue log.
    ///
    /// This is the main entry point for writing records to the queue log.
    /// The method handles the complete write process including:
    /// - Segment rolling if needed
    /// - Metadata updates
    /// - Record writing
    /// - Offset management
    ///
    /// # Arguments
    ///
    /// * `journal_records_batch` - Batch of records from the journal to append
    ///
    /// # Returns
    ///
    /// * `AppResult<LogAppendInfo>` - Information about the append operation
    ///
    /// # Concurrency
    ///
    /// This operation is thread-safe but may block other writers while:
    /// - Rolling segments
    /// - Updating metadata
    /// - Writing records
    pub async fn append_records(
        &self,
        journal_records_batch: JournalRecordsBatch,
    ) -> AppResult<LogAppendInfo> {
        let JournalRecordsBatch {
            journal_offset: _,
            queue_topic_partition,
            first_batch_queue_base_offset,
            last_batch_queue_base_offset,
            records_count,
            records: memory_records,
        } = journal_records_batch;

        trace!(
            "append records to queue log:{}, offset:{}",
            &queue_topic_partition,
            first_batch_queue_base_offset,
        );

        // check if need roll new segment
        if self.should_roll_segment(&memory_records).await? {
            self.roll_new_segment().await?;
        }

        // create log append info
        let log_append_info = self.create_log_append_info(
            first_batch_queue_base_offset,
            last_batch_queue_base_offset,
            records_count,
        );

        // update active segment metadata
        self.update_active_segment_metadata(&memory_records, log_append_info.first_offset)?;

        // execute write operation
        self.execute_write_operation(memory_records).await?;

        // update last offset
        self.update_last_offset(log_append_info.first_offset, records_count);

        trace!(
            "append records to queue log success, update last offset:{}",
            self.last_offset.load(Ordering::Acquire)
        );

        Ok(log_append_info)
    }

    /// Checks if a new segment should be created.
    ///
    /// Evaluates segment rolling criteria including:
    /// - Current segment size vs maximum
    /// - Index fullness
    /// - Incoming record size
    ///
    /// # Arguments
    ///
    /// * `memory_records` - Records to be written
    ///
    /// # Returns
    ///
    /// * `AppResult<bool>` - True if new segment needed
    async fn should_roll_segment(&self, memory_records: &MemoryRecords) -> AppResult<bool> {
        let active_seg_size =
            get_active_segment_writer().active_segment_size(&self.topic_partition);

        let active_segment_offset_index_full = self.active_segment_index.read().offset_index_full();

        Ok(self.need_roll(
            active_seg_size,
            memory_records,
            active_segment_offset_index_full,
        ))
    }

    /// Creates and switches to a new segment.
    ///
    /// This operation involves:
    /// 1. Flushing current segment
    /// 2. Creating new segment files
    /// 3. Updating segment references
    /// 4. Converting old segment to read-only
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - Success if segment roll completes
    ///
    /// # State Changes
    ///
    /// - Creates new active segment
    /// - Converts current segment to read-only
    /// - Updates segment maps and orders
    async fn roll_new_segment(&self) -> AppResult<()> {
        let new_base_offset = self.last_offset.load(Ordering::Acquire) + 1;

        let request = FlushRequest {
            topic_partition: self.topic_partition.clone(),
        };
        get_active_segment_writer().flush(request).await?;
        self.active_segment_index.write().flush_index()?;
        self.recover_point
            .store(self.last_offset.load(Ordering::Acquire), Ordering::Release);
        let old_base_offset = self.active_segment_id.load(Ordering::Acquire);

        // Create new segment
        let new_seg = ActiveSegmentIndex::new(
            &self.topic_partition,
            new_base_offset,
            global_config().log.queue_segment_size as usize,
        )?;
        // open new segment
        get_active_segment_writer().open_file(&self.topic_partition, new_base_offset)?;

        {
            // Swap active segment
            let mut active_seg = self.active_segment_index.write();
            let old_segment = std::mem::replace(&mut *active_seg, new_seg);
            self.active_segment_id
                .store(new_base_offset, Ordering::Release);

            // add old segment to segments
            let readonly_seg = old_segment.into_readonly()?;
            let mut segments_order = self.segments_order.write();
            segments_order.insert(new_base_offset);
            self.segments
                .insert(old_base_offset, Arc::new(readonly_seg));
        }

        trace!("after rolling new segment, self status={:?}", &self);

        Ok(())
    }

    /// Creates append information for the log operation.
    ///
    /// Generates metadata about the append operation including:
    /// - First and last offsets
    /// - Record counts
    /// - Timestamps
    ///
    /// # Arguments
    ///
    /// * `first_batch_queue_base_offset` - First offset in the batch
    /// * `last_batch_queue_base_offset` - Last offset in the batch
    /// * `records_count` - Number of records in the batch
    ///
    /// # Returns
    ///
    /// Information about the append operation
    fn create_log_append_info(
        &self,
        first_batch_queue_base_offset: i64,
        last_batch_queue_base_offset: i64,
        records_count: u32,
    ) -> LogAppendInfo {
        LogAppendInfo {
            first_offset: first_batch_queue_base_offset,
            _last_offset: last_batch_queue_base_offset,
            _max_timestamp: -1,
            _offset_of_max_timestamp: -1,
            _records_count: records_count,
            _log_append_time: DEFAULT_LOG_APPEND_TIME,
        }
    }

    /// Updates metadata for the active segment.
    ///
    /// Maintains index information including:
    /// - Offset mappings
    /// - Size information
    /// - Position tracking
    ///
    /// # Arguments
    ///
    /// * `memory_records` - Records being written
    /// * `first_offset` - First offset in the batch
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - Success if metadata updates
    fn update_active_segment_metadata(
        &self,
        memory_records: &MemoryRecords,
        first_offset: i64,
    ) -> AppResult<()> {
        let active_seg_size =
            get_active_segment_writer().active_segment_size(&self.topic_partition);
        self.active_segment_index.write().update_index(
            first_offset,
            memory_records.size(),
            LogType::Queue,
            active_seg_size,
        )
    }

    /// Performs the actual write operation to disk.
    ///
    /// Handles the low-level write operation including:
    /// - Creating write request
    /// - Sending to segment writer
    /// - Handling write errors
    ///
    /// # Arguments
    ///
    /// * `memory_records` - Records to write
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - Success if write completes
    async fn execute_write_operation(&self, memory_records: MemoryRecords) -> AppResult<()> {
        let queue_log_write_op = QueueFileWriteReq {
            topic_partition: self.topic_partition.clone(),
            records: memory_records,
        };

        get_active_segment_writer()
            .append_queue(queue_log_write_op)
            .await
    }

    /// Updates the last offset after a successful write.
    ///
    /// Atomically updates the last offset based on:
    /// - First offset in batch
    /// - Number of records written
    ///
    /// # Arguments
    ///
    /// * `first_offset` - First offset in the batch
    /// * `records_count` - Number of records written
    fn update_last_offset(&self, first_offset: i64, records_count: u32) {
        self.last_offset
            .store(first_offset + records_count as i64 - 1, Ordering::Release);
    }

    /// Closes the queue log, ensuring all data is flushed.
    ///
    /// Performs cleanup operations:
    /// 1. Closes active segment index
    /// 2. Flushes pending writes
    /// 3. Updates recovery point
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - Success if close completes
    pub async fn close(&self) -> AppResult<()> {
        self.active_segment_index.write().close()?;

        let request = FlushRequest {
            topic_partition: self.topic_partition.clone(),
        };
        get_active_segment_writer().flush(request).await?;

        self.recover_point
            .store(self.last_offset.load(Ordering::Acquire), Ordering::Release);
        Ok(())
    }

    /// Determines if a new segment should be created.
    ///
    /// Evaluates multiple criteria:
    /// - Current segment size vs configured maximum
    /// - Index capacity
    /// - Incoming record size
    ///
    /// # Arguments
    ///
    /// * `active_seg_size` - Current size of active segment
    /// * `memory_records` - Records to be written
    /// * `active_segment_offset_index_full` - Whether index is full
    ///
    /// # Returns
    ///
    /// `true` if new segment needed, `false` otherwise
    fn need_roll(
        &self,
        active_seg_size: u64,
        memory_records: &MemoryRecords,
        active_segment_offset_index_full: bool,
    ) -> bool {
        // debug!(
        //     "active_seg_size: {}, memory_records size: {}, global config size: {},active_segment_offset_index_full: {}",
        //     active_seg_size,
        //     memory_records.size() as u32,
        //     global_config().log.queue_segment_size as u32,
        //     active_segment_offset_index_full
        // );
        active_seg_size + memory_records.size() as u64
            >= global_config().log.queue_segment_size as u64
            || active_segment_offset_index_full
    }
}
