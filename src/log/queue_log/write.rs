use crate::log::log_segment::LogSegment;
use crate::log::{LogAppendInfo, LogType, DEFAULT_LOG_APPEND_TIME};
use crate::message::{MemoryRecords, TopicPartition};
use crate::{global_config, AppError, AppResult};
use tokio::sync::oneshot;
use tracing::{debug, trace};

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

        let (active_seg_size, active_segment_offset_index_full) = {
            let segments = self.segments.read().await;
            let active_seg = segments
                .iter()
                .next_back()
                .ok_or_else(|| self.no_active_segment_error(&self.topic_partition))?;
            (
                active_seg.1.size() as u32,
                active_seg.1.offset_index_full().await?,
            )
        };

        let need_roll = self
            .need_roll(
                active_seg_size,
                &memory_records,
                active_segment_offset_index_full,
            )
            .await;

        if need_roll {
            self.roll_segment().await?;
        }

        let log_append_info = LogAppendInfo {
            first_offset: first_batch_queue_base_offset,
            last_offset: last_batch_queue_base_offset,
            _max_timestamp: -1,
            _offset_of_max_timestamp: -1,
            records_count,
            _log_append_time: DEFAULT_LOG_APPEND_TIME,
        };

        self.append_to_active_segment(
            journal_offset,
            topic_partition,
            first_batch_queue_base_offset,
            last_batch_queue_base_offset,
            records_count,
            memory_records,
        )
        .await?;

        self.last_offset
            .store(log_append_info.first_offset + records_count as i64 - 1);
        trace!(
            "append records to queue log success, update last offset:{}",
            self.last_offset.load()
        );

        Ok(log_append_info)
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
        let segments = self.segments.write().await;
        let active_seg = segments
            .iter()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error(&self.topic_partition))?;
        active_seg.1.flush().await?;
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
    async fn need_roll(
        &self,
        active_seg_size: u32,
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
        active_seg_size + memory_records.size() as u32
            >= global_config().log.queue_segment_size as u32
            || active_segment_offset_index_full
    }

    /// Rolls over to a new log segment.
    ///
    /// # Returns
    ///
    /// Returns `AppResult<()>` indicating success or failure.
    ///
    /// # Performance
    ///
    /// This method acquires a write lock on the segments, which may impact concurrent operations.
    async fn roll_segment(&self) -> AppResult<()> {
        let mut segments = self.segments.write().await;
        let (_, active_seg) = segments
            .iter()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error(&self.topic_partition))?;

        active_seg.flush().await?;
        self.recover_point.store(self.last_offset.load());

        let new_base_offset = self.last_offset.load() + 1;

        let new_seg = LogSegment::new(
            &self.topic_partition,
            self.topic_partition.queue_partition_dir(),
            new_base_offset,
            self.index_file_max_size,
        )
        .await?;
        segments.insert(new_base_offset, new_seg);
        debug!(
            "Rolled queue log segment to new base offset: {}",
            new_base_offset
        );
        Ok(())
    }

    /// Appends records to the active segment.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition.
    /// * `memory_records` - The memory records to append.
    ///
    /// # Returns
    ///
    /// Returns `AppResult<()>` indicating success or failure.
    ///
    /// # Performance
    ///
    /// This method acquires a read lock on the segments, which may impact concurrent operations.
    async fn append_to_active_segment(
        &self,
        journal_offset: i64,
        topic_partition: TopicPartition,
        first_batch_queue_base_offset: i64,
        last_batch_queue_base_offset: i64,
        records_count: u32,
        memory_records: MemoryRecords,
    ) -> AppResult<()> {
        let segments = self.segments.read().await;
        let active_seg = segments
            .iter()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error(&self.topic_partition))?;

        let (tx, rx) = oneshot::channel();
        active_seg
            .1
            .append_record(
                LogType::Queue,
                (
                    journal_offset,
                    topic_partition,
                    first_batch_queue_base_offset,
                    last_batch_queue_base_offset,
                    records_count,
                    memory_records,
                    tx,
                ),
            )
            .await?;
        rx.await??;
        Ok(())
    }
    /// Creates an error for when no active segment is found.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition for which no active segment was found.
    ///
    /// # Returns
    ///
    /// Returns an `AppError` describing the error condition.
    pub fn no_active_segment_error(&self, topic_partition: &TopicPartition) -> AppError {
        AppError::IllegalStateError(format!(
            "no active segment, topic partition: {}",
            topic_partition
        ))
    }
}
