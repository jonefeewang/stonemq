//! Write operations for journal logs.
//!
//! This module provides functionality for appending records to journal logs,
//! including metadata management and segment rolling.

use std::sync::Arc;
use tracing::{debug, trace};

use crate::{
    global_config,
    log::{
        get_active_segment_writer,
        log_file_writer::{FlushRequest, JournalFileWriteReq},
        segment_index::ActiveSegmentIndex,
        LogAppendInfo, LogType, DEFAULT_LOG_APPEND_TIME,
    },
    message::{MemoryRecords, RecordBatch, TopicPartition},
    AppError, AppResult,
};

use super::JournalLog;

impl JournalLog {
    /// Appends records to the journal log.
    ///
    /// This method handles the complete process of appending records:
    /// 1. Adds metadata to the records
    /// 2. Creates a write operation
    /// 3. Writes the records to the active segment
    /// 4. Updates necessary offsets and indexes
    ///
    /// # Arguments
    ///
    /// * `memory_records` - The records to append
    /// * `queue_topic_partition` - The topic partition these records belong to
    /// * `reply_sender` - Channel sender for sending the append result
    ///
    /// # Note
    ///
    /// This method is async and handles its own error reporting through the reply_sender.
    ///
    /// In async Rust, it is not possible to hold a `MutexGuard` across an `.await`. Therefore,
    /// the asynchronous operations of the `LogFileWriter` in the active segment are elevated here,
    /// separating the locking operation from the asynchronous operation. This ensures that the
    /// synchronous lock is released before executing the asynchronous operation!
    pub async fn append_records(
        &self,
        mut memory_records: MemoryRecords,
        queue_topic_partition: &TopicPartition,
    ) -> AppResult<LogAppendInfo> {
        // Add metadata and validate records
        let log_append_info = match self
            .process_append_request(&mut memory_records, queue_topic_partition)
            .await
        {
            Ok(info) => info,
            Err(e) => return Err(e),
        };

        // Create and execute write operation
        let journal_log_write_op = JournalFileWriteReq {
            journal_offset: self.next_offset.load(),
            topic_partition: self.topic_partition.clone(),
            queue_topic_partition: queue_topic_partition.clone(),
            first_batch_queue_base_offset: log_append_info.first_offset,
            last_batch_queue_base_offset: log_append_info._last_offset,
            records_count: log_append_info._records_count,
            records: memory_records,
        };

        trace!("append_journal");

        if let Err(e) = get_active_segment_writer()
            .append_journal(journal_log_write_op)
            .await
        {
            return Err(AppError::DetailedIoError(e.to_string()));
        }

        debug!(
            " append journal log success with journal offset {} of topic_partition {}",
            self.next_offset.load(),
            self.topic_partition.id()
        );

        // Update offsets and send success response
        self.next_offset.fetch_add(1);
        Ok(log_append_info)
    }

    /// Process append request
    /// Adds metadata to records and validates them.
    ///
    /// # Arguments
    ///
    /// * `memory_records` - Records to process
    /// * `queue_topic_partition` - Associated topic partition
    ///
    /// # Returns
    ///
    /// Returns information about the append operation if successful
    async fn process_append_request(
        &self,
        memory_records: &mut MemoryRecords,
        queue_topic_partition: &TopicPartition,
    ) -> AppResult<LogAppendInfo> {
        // Extract record batches
        let record_batches = memory_records.into_iter().collect::<Vec<_>>();

        // Validate and assign offsets
        let log_append_info = self.validate_records_and_assign_queue_offset(
            queue_topic_partition,
            record_batches,
            memory_records,
        )?;

        // Log debug information
        if let Some(offset_info) = self.queue_next_offset_info.get(queue_topic_partition) {
            trace!(
                "topic_partition={} append_complete next_offset={} entry_value={:?}",
                queue_topic_partition.id(),
                self.next_offset.load(),
                *offset_info
            );
        }

        // Check if segment rolling is needed
        let active_segment_offset_index_full = self.active_segment_index.read().offset_index_full();
        let active_seg_size =
            get_active_segment_writer().active_segment_size(&self.topic_partition);


        trace!("active_segment_offset_index_full={}", active_segment_offset_index_full);

        if self.need_roll(
            active_seg_size as u32,
            memory_records,
            active_segment_offset_index_full,
        ) {
            trace!("roll_active_segment");
            self.roll_active_segment().await?;
        }

        // Update active segment metadata
        {
            trace!("update_index");
            self.active_segment_index.write().update_index(
                self.next_offset.load(),
                memory_records.size(),
                LogType::Journal,
                get_active_segment_writer().active_segment_size(&self.topic_partition),
            )?;
        }
        trace!("update_index complete");

        Ok(log_append_info)
    }

    /// Rolls the active segment to a new one.
    ///
    /// This method handles the process of creating a new segment and
    /// transitioning the current active segment to a read-only state.
    async fn roll_active_segment(&self) -> AppResult<()> {
        let new_base_offset = self.next_offset.load();

        // Flush old segment
        get_active_segment_writer()
            .flush(FlushRequest {
                topic_partition: self.topic_partition.clone(),
            })
            .await?;
        self.active_segment_index.write().flush_index()?;
        self.recover_point.store(self.next_offset.load() - 1);
        let old_base_offset = self.active_segment_base_offset.load();

        // Create new segment
        let new_seg = ActiveSegmentIndex::new(
            &self.topic_partition,
            new_base_offset,
            global_config().log.journal_segment_size as usize,
        )?;
        // open new segment
        get_active_segment_writer().open_file(&self.topic_partition, new_base_offset)?;

        {
            // Swap active segment index
            let mut active_seg_index = self.active_segment_index.write();
            let old_segment_index = std::mem::replace(&mut *active_seg_index, new_seg);
            self.active_segment_base_offset.store(new_base_offset);
            // add old segment to segments
            let readonly_seg = old_segment_index.into_readonly()?;
            let mut segments_order = self.segments_order.write();
            segments_order.insert(new_base_offset);
            self.segment_index
                .insert(old_base_offset, Arc::new(readonly_seg));
        }

        debug!("Rolled segment: self status={:?}", &self);

        Ok(())
    }

    /// Validates records and assigns queue offsets.
    ///
    /// # Arguments
    ///
    /// * `queue_topic_partition` - Topic partition for the queue
    /// * `record_batches` - Record batches to process
    /// * `memory_records` - Memory records to update
    ///
    /// # Returns
    ///
    /// Returns append information if validation succeeds
    fn validate_records_and_assign_queue_offset(
        &self,
        queue_topic_partition: &TopicPartition,
        record_batches: Vec<RecordBatch>,
        memory_records: &mut MemoryRecords,
    ) -> AppResult<LogAppendInfo> {
        let mut max_timestamp = -1i64;
        let mut offset_of_max_timestamp = -1i64;
        let mut first_offset = -1i64;
        let mut last_offset = -1i64;
        let mut records_count = 0u32;

        let mut queue_next_offset = self
            .queue_next_offset_info
            .entry(queue_topic_partition.clone())
            .or_insert(0);

        trace!(
            "validate_records start: next_offset={}, topic_partition={}",
            *queue_next_offset,
            queue_topic_partition.id()
        );

        if first_offset == -1 {
            first_offset = *queue_next_offset;
        }

        // Process each batch
        for mut batch in record_batches {
            batch.validate_batch()?;

            // Process records in batch
            for record in batch.records() {
                records_count += 1;
                *queue_next_offset += 1;

                let record_timestamp = batch.base_timestamp() + record.timestamp_delta;
                if record_timestamp > max_timestamp {
                    max_timestamp = record_timestamp;
                    offset_of_max_timestamp = *queue_next_offset;
                }
            }

            batch.set_base_offset(*queue_next_offset - batch.last_offset_delta() as i64 - 1);
            last_offset = batch.base_offset();

            trace!(
                "batch: topic_partition={}, base_offset={}, next_offset={}",
                queue_topic_partition.id(),
                batch.base_offset(),
                *queue_next_offset
            );

            batch.unsplit(memory_records);
        }

        trace!(
            "validate_records complete: next_offset={}",
            *queue_next_offset
        );

        Ok(LogAppendInfo {
            first_offset,
            _last_offset: last_offset,
            _max_timestamp: max_timestamp,
            _offset_of_max_timestamp: offset_of_max_timestamp,
            _records_count: records_count,
            _log_append_time: DEFAULT_LOG_APPEND_TIME,
        })
    }

    pub async fn close(&self) -> AppResult<()> {
        self.active_segment_index.write().close()?;

        let request = FlushRequest {
            topic_partition: self.topic_partition.clone(),
        };
        get_active_segment_writer().flush(request).await?;

        self.recover_point.store(self.next_offset.load() - 1);

        debug!(
            "Flushed segment: topic_partition={} offset={} recover_point={}",
            self.topic_partition.id(),
            self.next_offset.load(),
            self.recover_point.load()
        );

        Ok(())
    }

    /// Checks if the active segment needs to be rolled.
    ///
    /// # Returns
    ///
    /// Returns true if the segment should be rolled, false otherwise
    fn need_roll(
        &self,
        active_seg_size: u32,
        memory_records: &MemoryRecords,
        active_segment_offset_index_full: bool,
    ) -> bool {
        let config = &global_config().log;
        let total_size = Self::calculate_journal_log_overhead(&self.topic_partition)
            + memory_records.size() as u32
            + active_seg_size;

        let should_roll =
            total_size >= config.journal_segment_size as u32 || active_segment_offset_index_full;

        if should_roll {
            debug!(
                "Rolling segment: total_size={}, config_size={}, index_full={}",
                total_size, config.journal_segment_size, active_segment_offset_index_full
            );
        }

        should_roll
    }
}
