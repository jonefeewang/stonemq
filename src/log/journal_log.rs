use std::collections::BTreeMap;
use std::sync::Arc;

use crossbeam_utils::atomic::AtomicCell;
use dashmap::DashMap;
use tokio::sync::{oneshot, RwLock};
use tracing::{info, trace};

use super::log_segment::PositionInfo;
use super::{calculate_journal_log_overhead, Log};
use crate::log::log_segment::LogSegment;
use crate::message::{LogAppendInfo, MemoryRecords, TopicPartition};
use crate::AppError::{self, CommonError};
use crate::{global_config, AppResult};

/// Represents a log manager for a log partition.
///
/// Responsible for managing log segments, appending records, and rolling log segments.
#[derive(Debug)]
pub struct JournalLog {
    segments: RwLock<BTreeMap<u64, Arc<LogSegment>>>, // RwLock for segments allows multiple reads, single write
    queue_next_offset_info: DashMap<TopicPartition, i64>, // DashMap for concurrent-safe hash map
    log_start_offset: AtomicCell<u64>,
    next_offset: AtomicCell<u64>,
    recover_point: AtomicCell<u64>,
    pub split_offset: AtomicCell<u64>,
    write_lock: RwLock<()>, // Mutex for write operations
    topic_partition: TopicPartition,
}

impl Log for JournalLog {
    /// Appends records to the log.
    ///
    /// # Arguments
    ///
    /// * `records` - A tuple containing the topic partition and memory records to append.
    ///
    /// # Returns
    ///
    /// Returns `AppResult<LogAppendInfo>` containing information about the append operation.
    async fn append_records(
        &self,
        records: (TopicPartition, MemoryRecords),
    ) -> AppResult<LogAppendInfo> {
        let (topic_partition, mut memory_records) = records;
        let _write_guard = self.write_lock.write().await; // Acquire write lock for atomic append operation

        let (active_seg_size, active_segment_offset_index_full) =
            self.get_active_segment_info().await?;

        if self
            .need_roll(
                &topic_partition,
                active_seg_size,
                &memory_records,
                active_segment_offset_index_full,
            )
            .await
        {
            self.roll_segment().await?;
        }

        let records_count = memory_records.records_count() as u32;

        self.assign_offset(&topic_partition, &mut memory_records)
            .await?;
        self.append_to_active_segment(topic_partition, memory_records)
            .await?;

        let log_append_info = LogAppendInfo {
            base_offset: self.next_offset.load() as i64,
            log_append_time: -1,
        };

        self.update_offsets(records_count).await;

        Ok(log_append_info)
    }
}

impl JournalLog {
    /// Creates a new JournalLog instance.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition for this log.
    /// * `segments` - A BTreeMap of existing log segments.
    /// * `log_start_offset` - The starting offset for this log.
    /// * `log_recovery_point` - The recovery point offset.
    /// * `split_offset` - The split offset.
    ///
    /// # Returns
    ///
    /// Returns `AppResult<Self>` containing the new JournalLog instance.
    pub async fn new(
        topic_partition: TopicPartition,
        segments: BTreeMap<u64, Arc<LogSegment>>,
        log_start_offset: u64,
        log_recovery_point: u64,
        split_offset: u64,
    ) -> AppResult<Self> {
        let dir = topic_partition.journal_log_dir();
        tokio::fs::create_dir_all(&dir).await?;

        let segments = if segments.is_empty() {
            info!("No segment file found in journal log dir: {}", dir);
            let segment = Arc::new(LogSegment::new_journal_seg(&topic_partition, 0, true).await?);
            let mut segments = BTreeMap::new();
            segments.insert(0, segment);
            segments
        } else {
            segments
        };

        Ok(Self {
            segments: RwLock::new(segments),
            queue_next_offset_info: DashMap::new(),
            log_start_offset: AtomicCell::new(log_start_offset),
            next_offset: AtomicCell::new(log_recovery_point + 1),
            recover_point: AtomicCell::new(log_recovery_point),
            split_offset: AtomicCell::new(split_offset),
            write_lock: RwLock::new(()),
            topic_partition,
        })
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
    pub async fn flush(&self, active_segment: &LogSegment) -> AppResult<()> {
        active_segment.flush().await?;
        self.recover_point.store(self.next_offset.load());
        Ok(())
    }

    /// Retrieves position information for a given offset.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset to get position information for.
    ///
    /// # Returns
    ///
    /// Returns `AppResult<PositionInfo>` containing the position information.
    pub async fn get_position_info(&self, offset: u64) -> AppResult<PositionInfo> {
        let segments = self.segments.read().await; // Acquire read lock for concurrent reads
        let segment = segments
            .range(..=offset)
            .next_back()
            .map(|(_, segment)| segment)
            .ok_or_else(|| CommonError(format!("No segment found for offset {}", offset)))?;
        segment.get_position(offset).await
    }

    /// Gets the offset of the current active segment.
    ///
    /// # Returns
    ///
    /// Returns `AppResult<u64>` containing the active segment's offset.
    pub async fn current_active_seg_offset(&self) -> AppResult<u64> {
        let segments = self.segments.read().await; // Acquire read lock for concurrent reads
        let active_seg = segments
            .values()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error())?;
        Ok(active_seg.base_offset())
    }

    /// Retrieves information about the active segment.
    ///
    /// # Returns
    ///
    /// Returns `AppResult<(u32, bool)>` containing the segment size and whether the offset index is full.
    async fn get_active_segment_info(&self) -> AppResult<(u32, bool)> {
        let segments = self.segments.read().await; // Acquire read lock for concurrent reads
        let active_seg = segments
            .values()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error())?;
        Ok((
            active_seg.size() as u32,
            active_seg.offset_index_full().await?,
        ))
    }

    /// Determines if a new log segment roll is needed.
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
    /// Returns a boolean indicating whether a roll is needed.
    async fn need_roll(
        &self,
        topic_partition: &TopicPartition,
        active_seg_size: u32,
        memory_records: &MemoryRecords,
        active_segment_offset_index_full: bool,
    ) -> bool {
        let config = &global_config().log;
        let total_size = calculate_journal_log_overhead(topic_partition)
            + memory_records.size() as u32
            + active_seg_size;

        total_size >= config.journal_segment_size as u32 || active_segment_offset_index_full
    }

    /// Rolls over to a new log segment.
    ///
    /// # Returns
    ///
    /// Returns `AppResult<()>` indicating success or failure.
    async fn roll_segment(&self) -> AppResult<()> {
        let mut segments = self.segments.write().await; // Acquire write lock for atomic roll operation
        let (_, active_seg) = segments
            .iter()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error())?;

        self.flush(active_seg).await?;
        let new_base_offset = self.next_offset.load();

        let new_seg = Arc::new(
            LogSegment::new_journal_seg(&self.topic_partition, new_base_offset, true).await?,
        );
        segments.insert(new_base_offset, new_seg);
        trace!(
            "Rolled journal log segment to new base offset: {}",
            new_base_offset
        );
        Ok(())
    }

    /// Assigns offsets to the memory records.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition.
    /// * `memory_records` - The memory records to assign offsets to.
    ///
    /// # Returns
    ///
    /// Returns `AppResult<()>` indicating success or failure.
    async fn assign_offset(
        &self,
        topic_partition: &TopicPartition,
        memory_records: &mut MemoryRecords,
    ) -> AppResult<()> {
        let queue_log_next_offset = self
            .queue_next_offset_info
            .entry(topic_partition.clone())
            .or_insert(0);
        let offset = *queue_log_next_offset;
        memory_records.set_base_offset(offset)?;
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
    async fn append_to_active_segment(
        &self,
        topic_partition: TopicPartition,
        memory_records: MemoryRecords,
    ) -> AppResult<()> {
        let segments = self.segments.read().await; // Acquire read lock for concurrent reads
        let active_seg = segments
            .values()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error())?;

        let (tx, rx) = oneshot::channel();
        active_seg
            .append_record((self.next_offset.load(), topic_partition, memory_records, tx))
            .await?;
        rx.await??;
        Ok(())
    }

    /// Updates offset information after appending records.
    ///
    /// # Arguments
    ///
    /// * `record_count` - The number of records appended.
    async fn update_offsets(&self, record_count: u32) {
        let mut queue_next_offset = self
            .queue_next_offset_info
            .entry(self.topic_partition.clone())
            .or_insert(0);
        *queue_next_offset += record_count as i64;

        self.next_offset.fetch_add(record_count as u64);
    }

    /// Determines the base offset of the next segment.
    ///
    /// This method calculates the base offset for the next segment based on the
    /// provided current segment's base offset. It searches for the segment with
    /// the next higher base offset in the segments map.
    ///
    /// # Arguments
    ///
    /// * `current_base_offset` - The base offset of the current segment.
    ///
    /// # Returns
    ///
    /// Returns the base offset of the next segment. If there is no next segment
    /// (i.e., the current segment is the last one), it returns the next offset
    /// of the log, which would be the base offset for a new segment if one were
    /// to be created.
    ///
    /// # Performance
    ///
    /// This method acquires a read lock on the segments map, which may briefly
    /// impact concurrent operations.
    pub async fn next_segment_base_offset(&self, current_base_offset: u64) -> u64 {
        let segments = self.segments.read().await;
        segments
            .range((current_base_offset + 1)..)
            .next()
            .map(|(&base_offset, _)| base_offset)
            .unwrap_or_else(|| self.next_offset.load())
    }

    /// Generates an error for when no active segment is found.
    ///
    /// # Returns
    ///
    /// Returns an `AppError` indicating no active segment was found.
    fn no_active_segment_error(&self) -> AppError {
        CommonError(format!(
            "No active segment for topic partition: {}",
            self.topic_partition
        ))
    }
}
