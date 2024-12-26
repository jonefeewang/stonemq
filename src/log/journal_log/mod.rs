//! Journal log implementation for managing log segments and records.
//!
//! This module provides functionality for:
//! - Managing multiple log segments
//! - Appending records to active segments
//! - Rolling segments when they reach capacity
//! - Maintaining offset information
mod journal_load;
mod journal_read;
mod journal_write;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
use parking_lot::RwLock;

use crate::{
    global_config,
    log::{CheckPointFile, NEXT_OFFSET_CHECKPOINT_FILE_NAME},
    message::TopicPartition,
    AppError, AppResult,
};

use super::{
    segment_index::{ActiveSegmentIndex, ReadOnlySegmentIndex, SegmentIndexCommon},
    ActiveSegmentWriter,
};

struct JournalLogMetadata {
    queue_next_offset_info: DashMap<TopicPartition, i64>,
    queue_next_offset_checkpoints: CheckPointFile,
    log_start_offset: i64,
    next_offset: i64,
    recover_point: i64,
    split_offset: i64,
}

/// Represents a journal log manager for a log partition.
///
/// Responsible for:
/// - Managing log segments (both active and inactive)
/// - Appending records
/// - Rolling segments
/// - Maintaining offset information
#[derive(Debug)]
pub struct JournalLog {
    /// Ordered set of segment base offsets (non-active segments)
    /// Write lock only needed when adding new segments or cleaning old ones
    segments_order: RwLock<BTreeSet<i64>>,

    /// Map of base offsets to read-only segments
    segment_index: DashMap<i64, Arc<ReadOnlySegmentIndex>>,

    /// Currently active segment
    active_segment_index: RwLock<ActiveSegmentIndex>,

    /// Base offset of current active segment
    active_segment_base_offset: AtomicCell<i64>,

    /// Next offset information for queues
    queue_next_offset_info: DashMap<TopicPartition, i64>,

    /// Starting offset of the log
    _log_start_offset: AtomicCell<i64>,

    /// Next offset to be assigned
    pub(crate) next_offset: AtomicCell<i64>,

    /// Recovery point offset
    pub(crate) recover_point: AtomicCell<i64>,

    /// Split point offset
    pub split_offset: AtomicCell<i64>,

    /// Topic partition information
    topic_partition: TopicPartition,

    /// Checkpoint file for queue next offsets
    queue_next_offset_checkpoints: CheckPointFile,

    /// Active segment writer
    active_segment_writer: Arc<ActiveSegmentWriter>,
}

impl JournalLog {
    /// Initial values for various offsets
    const INIT_LOG_START_OFFSET: i64 = 0;
    const INIT_RECOVER_POINT: i64 = -1;
    const INIT_SPLIT_OFFSET: i64 = -1;
    const INIT_NEXT_OFFSET: i64 = 0;

    /// Creates a new journal log instance.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - Topic partition this log belongs to
    ///
    /// # Returns
    ///
    /// Returns a new `JournalLog` instance or an error if creation fails
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Directory creation fails
    /// - Initial segment creation fails
    pub fn new(
        topic_partition: &TopicPartition,
        active_segment_writer: Arc<ActiveSegmentWriter>,
    ) -> AppResult<Self> {
        let dir = topic_partition.partition_dir();
        std::fs::create_dir_all(&dir).map_err(|e| {
            AppError::DetailedIoError(format!(
                "failed to create journal partition directory {}: {}",
                dir, e
            ))
        })?;

        let index_file_max_size = global_config().log.journal_index_file_size;
        let active_segment_index = ActiveSegmentIndex::new(
            topic_partition,
            Self::INIT_LOG_START_OFFSET,
            index_file_max_size as usize,
        )?;

        // open active segment writer
        active_segment_writer.open_file(topic_partition, active_segment_index.base_offset())?;

        Ok(Self {
            segments_order: RwLock::new(BTreeSet::new()),
            segment_index: DashMap::new(),
            active_segment_index: RwLock::new(active_segment_index),
            active_segment_base_offset: AtomicCell::new(0),
            queue_next_offset_info: DashMap::new(),
            queue_next_offset_checkpoints: CheckPointFile::new(format!(
                "{}/{}",
                &topic_partition.partition_dir(),
                NEXT_OFFSET_CHECKPOINT_FILE_NAME
            )),
            _log_start_offset: AtomicCell::new(Self::INIT_LOG_START_OFFSET),
            next_offset: AtomicCell::new(Self::INIT_NEXT_OFFSET),
            recover_point: AtomicCell::new(Self::INIT_RECOVER_POINT),
            split_offset: AtomicCell::new(Self::INIT_SPLIT_OFFSET),
            topic_partition: topic_partition.clone(),
            active_segment_writer,
        })
    }

    /// Opens an existing journal log.
    ///
    /// # Arguments
    ///
    /// * `segments` - Existing log segments
    /// * `active_segment` -  active segment
    /// * `queue_next_offset_info` - Next offset information for queues
    /// * `queue_next_offset_checkpoints` - Checkpoint file for queue offsets
    /// * `log_start_offset` - Starting offset of the log
    /// * `next_offset` - Next offset to be assigned
    /// * `recover_point` - Recovery point offset
    /// * `split_offset` - Split point offset
    /// * `topic_partition` - Topic partition information
    ///
    /// # Returns
    ///
    /// Returns the opened `JournalLog` instance or an error if opening fails
    fn open(
        segments: BTreeMap<i64, ReadOnlySegmentIndex>,
        active_segment: ActiveSegmentIndex,
        topic_partition: &TopicPartition,
        metadata: JournalLogMetadata,
        active_segment_writer: Arc<ActiveSegmentWriter>,
    ) -> AppResult<Self> {
        let segments_order = segments.keys().cloned().collect();
        let segments_map = segments
            .into_iter()
            .map(|(k, v)| (k, Arc::new(v)))
            .collect();

        let active_segment_base_offset = active_segment.base_offset();

        // open active segment writer
        active_segment_writer.open_file(topic_partition, active_segment_base_offset)?;

        Ok(Self {
            segments_order: RwLock::new(segments_order),
            segment_index: segments_map,
            active_segment_index: RwLock::new(active_segment),
            active_segment_base_offset: AtomicCell::new(active_segment_base_offset),
            queue_next_offset_info: metadata.queue_next_offset_info,
            queue_next_offset_checkpoints: metadata.queue_next_offset_checkpoints,
            _log_start_offset: AtomicCell::new(metadata.log_start_offset),
            next_offset: AtomicCell::new(metadata.next_offset),
            recover_point: AtomicCell::new(metadata.recover_point),
            split_offset: AtomicCell::new(metadata.split_offset),
            topic_partition: topic_partition.clone(),
            active_segment_writer,
        })
    }
}
