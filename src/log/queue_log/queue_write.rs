use std::sync::Arc;

use crate::log::log_file_writer::{FlushRequest, QueueFileWriteReq};
use crate::log::segment_index::ActiveSegmentIndex;
use crate::log::{get_active_segment_writer, LogAppendInfo, LogType, DEFAULT_LOG_APPEND_TIME};
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
            _,
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
        self.execute_write_operation(memory_records).await?;

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
        let active_seg_size =
            get_active_segment_writer().active_segment_size(&self.topic_partition);

        let active_segment_offset_index_full = self.active_segment_index.read().offset_index_full();

        Ok(self.need_roll(
            active_seg_size,
            memory_records,
            active_segment_offset_index_full,
        ))
    }

    /// roll new segment
    async fn roll_new_segment(&self) -> AppResult<()> {
        let new_base_offset = self.last_offset.load() + 1;

        let request = FlushRequest {
            topic_partition: self.topic_partition.clone(),
        };
        get_active_segment_writer().flush(request).await?;
        self.active_segment_index.write().flush_index()?;
        self.recover_point.store(self.last_offset.load());
        let old_base_offset = self.active_segment_id.load();

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
            self.active_segment_id.store(new_base_offset);

            // add old segment to segments
            let readonly_seg = old_segment.into_readonly()?;
            let mut segments_order = self.segments_order.write();
            segments_order.insert(old_base_offset);
            self.segments
                .insert(old_base_offset, Arc::new(readonly_seg));
        }

        Ok(())
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
            _last_offset: last_batch_queue_base_offset,
            _max_timestamp: -1,
            _offset_of_max_timestamp: -1,
            _records_count: records_count,
            _log_append_time: DEFAULT_LOG_APPEND_TIME,
        }
    }

    /// update active segment metadata
    fn update_active_segment_metadata(
        &self,
        memory_records: &MemoryRecords,
        first_offset: i64,
    ) -> AppResult<()> {
        let active_seg_size =
            get_active_segment_writer().active_segment_size(&self.topic_partition);
        self.active_segment_index.write().update_index(
            memory_records.size(),
            first_offset,
            LogType::Queue,
            active_seg_size,
        )
    }

    /// execute write operation
    async fn execute_write_operation(&self, memory_records: MemoryRecords) -> AppResult<()> {
        let queue_log_write_op = QueueFileWriteReq {
            topic_partition: self.topic_partition.clone(),
            records: memory_records,
        };

        get_active_segment_writer()
            .append_queue(queue_log_write_op)
            .await
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
        self.active_segment_index.write().flush_index()?;

        let request = FlushRequest {
            topic_partition: self.topic_partition.clone(),
        };
        get_active_segment_writer().flush(request).await?;

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
