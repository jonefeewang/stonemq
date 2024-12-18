use std::sync::Arc;

use crate::log::file_writer::{FlushRequest, QueueLogWriteOp};
use crate::log::log_segment::{ActiveLogSegment, LogSegmentCommon};
use crate::log::{LogAppendInfo, LogType, DEFAULT_LOG_APPEND_TIME, FILE_WRITER};
use crate::message::{MemoryRecords, TopicPartition};
use crate::{global_config, AppResult};
use tracing::trace;

use super::QueueLog;

impl QueueLog {
    /// Appends records to the queue log.
    ///
    /// # Arguments
    ///
    /// * `records` - A tuple containing the topic partition and memory records to append.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing `LogAppendInfo` on success.
    ///
    /// # Performance
    ///
    /// This method acquires a write lock on the entire log, which may impact concurrent operations.
    pub async fn append_records(
        &self,
        records: (i64, TopicPartition, i64, i64, u32, MemoryRecords),
    ) -> AppResult<LogAppendInfo> {
        let (
            journal_offset,
            topic_partition,
            first_batch_queue_base_offset,
            last_batch_queue_base_offset,
            records_count,
            memory_records,
        ) = records;

        trace!(
            "append records to queue log:{}, offset:{}",
            &topic_partition,
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
        self.execute_write_operation(
            journal_offset,
            first_batch_queue_base_offset,
            last_batch_queue_base_offset,
            records_count,
            memory_records,
        )
        .await?;

        // update last offset
        self.update_last_offset(log_append_info.first_offset, records_count);

        trace!(
            "append records to queue log success, update last offset:{}",
            self.last_offset.load()
        );

        Ok(log_append_info)
    }

    /// check if need roll new segment
    async fn should_roll_segment(&self, memory_records: &MemoryRecords) -> AppResult<bool> {
        let (active_segment_size, active_segment_offset_index_full) = {
            let active_segment = self.active_segment.read();
            let active_segment_size = active_segment.size();
            let active_segment_offset_index_full = active_segment.offset_index_full();
            (active_segment_size, active_segment_offset_index_full)
        };

        Ok(self.need_roll(
            active_segment_size,
            memory_records,
            active_segment_offset_index_full,
        ))
    }

    /// roll new segment
    async fn roll_new_segment(&self) -> AppResult<()> {
        let new_base_offset = self.last_offset.load() + 1;
        let new_seg = ActiveLogSegment::new(
            &self.topic_partition,
            new_base_offset,
            self.index_file_max_size,
        )?;

        // swap active segment
        let old_segment = self.swap_active_segment(new_seg)?;

        // handle old segment
        let old_base_offset = old_segment.base_offset();
        let readonly_seg = old_segment.into_readonly()?;
        self.segments
            .insert(old_base_offset, Arc::new(readonly_seg));

        // flush old segment
        self.flush_old_segment(old_base_offset).await?;

        // update metadata
        self.update_segment_metadata(new_base_offset);

        Ok(())
    }

    /// swap active segment
    fn swap_active_segment(&self, new_seg: ActiveLogSegment) -> AppResult<ActiveLogSegment> {
        let mut active_seg = self.active_segment.write();
        Ok(std::mem::replace(&mut *active_seg, new_seg))
    }

    /// flush old segment
    async fn flush_old_segment(&self, old_base_offset: i64) -> AppResult<()> {
        let request = FlushRequest {
            topic_partition: self.topic_partition.clone(),
            segment_base_offset: old_base_offset,
        };
        FILE_WRITER.flush(request).await?;
        Ok(())
    }

    /// update segment metadata
    fn update_segment_metadata(&self, new_base_offset: i64) {
        self.recover_point.store(self.last_offset.load());
        {
            let mut segments = self.segments_order.write();
            segments.insert(new_base_offset);
        }
        self.active_segment_id.store(new_base_offset);
    }

    /// create log append info
    fn create_log_append_info(
        &self,
        first_batch_queue_base_offset: i64,
        last_batch_queue_base_offset: i64,
        records_count: u32,
    ) -> LogAppendInfo {
        LogAppendInfo {
            first_offset: first_batch_queue_base_offset,
            last_offset: last_batch_queue_base_offset,
            _max_timestamp: -1,
            _offset_of_max_timestamp: -1,
            records_count,
            _log_append_time: DEFAULT_LOG_APPEND_TIME,
        }
    }

    /// update active segment metadata
    fn update_active_segment_metadata(
        &self,
        memory_records: &MemoryRecords,
        first_offset: i64,
    ) -> AppResult<()> {
        self.active_segment.write().update_metadata(
            memory_records.size(),
            first_offset,
            LogType::Journal,
        )
    }

    /// execute write operation
    async fn execute_write_operation(
        &self,
        journal_offset: i64,
        first_batch_queue_base_offset: i64,
        last_batch_queue_base_offset: i64,
        records_count: u32,
        memory_records: MemoryRecords,
    ) -> AppResult<()> {
        let queue_log_write_op = QueueLogWriteOp {
            journal_offset,
            topic_partition: self.topic_partition.clone(),
            first_batch_queue_base_offset,
            last_batch_queue_base_offset,
            records_count,
            records: memory_records,
            segment_base_offset: self.active_segment_id.load(),
        };

        FILE_WRITER.append_queue(queue_log_write_op).await
    }

    /// update last offset
    fn update_last_offset(&self, first_offset: i64, records_count: u32) {
        self.last_offset
            .store(first_offset + records_count as i64 - 1);
    }

    /// Flushes the active segment to disk.
    ///
    /// # Arguments
    ///
    /// * `active_segment` - The active LogSegment to flush.
    ///
    /// # Returns
    ///
    /// Returns `AppResult<()>` indicating success or failure.
    pub async fn flush(&self) -> AppResult<()> {
        self.active_segment.write().flush_index()?;

        let request = FlushRequest {
            topic_partition: self.topic_partition.clone(),
            segment_base_offset: self.active_segment_id.load(),
        };
        FILE_WRITER.flush(request).await?;

        self.recover_point.store(self.last_offset.load());
        Ok(())
    }

    /// Determines if a new segment roll is needed.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition.
    /// * `active_seg_size` - The size of the active segment.
    /// * `memory_records` - The memory records to be appended.
    /// * `active_segment_offset_index_full` - Whether the active segment's offset index is full.
    ///
    /// # Returns
    ///
    /// Returns a boolean indicating whether a new segment roll is needed.
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
