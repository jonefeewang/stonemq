//! Queue log implementation for managing log segments and records.
//!
//! This module provides functionality for:
//! - Managing multiple log segments
//! - Appending records to active segments
//! - Rolling segments when they reach capacity
//! - Maintaining offset information

mod queue_load;
mod queue_read;
mod queue_write;

use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
    path::Path,
    sync::Arc,
};

use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::info;

use crate::{
    global_config,
    log::segment_index::{ActiveSegmentIndex, ReadOnlySegmentIndex, SegmentIndexCommon},
    message::TopicPartition,
    AppError, AppResult,
};

use super::ActiveSegmentWriter;

/// Constants for queue log operations
const INIT_LOG_START_OFFSET: i64 = 0;
const INIT_RECOVER_POINT: i64 = -1;
const INIT_LAST_OFFSET: i64 = 0;

/// Represents a queue log manager for a log partition.
///
/// Responsible for:
/// - Managing log segments (both active and inactive)
/// - Appending records
/// - Rolling segments
/// - Maintaining offset information
#[derive(Debug)]
pub struct QueueLog {
    /// Map of segment base offsets to read-only segments
    segments: DashMap<i64, Arc<ReadOnlySegmentIndex>>,

    /// Ordered set of segment base offsets
    /// Write lock only needed when adding new segments
    segments_order: RwLock<BTreeSet<i64>>,

    /// Currently active segment for writing
    active_segment_index: RwLock<ActiveSegmentIndex>,

    /// Base offset of the active segment
    active_segment_id: AtomicCell<i64>,

    /// Topic partition this log belongs to
    topic_partition: TopicPartition,

    /// First valid offset in the log
    log_start_offset: i64,

    /// Last known valid offset (for recovery)
    recover_point: AtomicCell<i64>,

    /// Last offset in the log
    last_offset: AtomicCell<i64>,

    /// Active segment writer
    active_segment_writer: Arc<ActiveSegmentWriter>,
}

impl Hash for QueueLog {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic_partition.hash(state);
    }
}

impl PartialEq for QueueLog {
    fn eq(&self, other: &Self) -> bool {
        self.topic_partition == other.topic_partition
    }
}

impl Eq for QueueLog {}

impl QueueLog {
    /// Creates a new empty queue log
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition this log belongs to
    ///
    /// # Returns
    ///
    /// A new QueueLog instance or an error if creation fails
    pub fn new(
        topic_partition: &TopicPartition,
        active_segment_writer: Arc<ActiveSegmentWriter>,
    ) -> AppResult<Self> {
        let dir = topic_partition.partition_dir();
        Self::ensure_dir_exists(&dir)?;

        let index_file_max_size = global_config().log.queue_index_file_size;
        let active_segment_index = ActiveSegmentIndex::new(topic_partition, 0, index_file_max_size)?;

        // open active segment writer
        active_segment_writer
            .open_file(topic_partition, active_segment_index.base_offset())?;

        Ok(Self {
            topic_partition: topic_partition.clone(),
            segments: DashMap::new(),
            segments_order: RwLock::new(BTreeSet::new()),
            active_segment_index: RwLock::new(active_segment_index),
            active_segment_id: AtomicCell::new(0),
            log_start_offset: INIT_LOG_START_OFFSET,
            recover_point: AtomicCell::new(INIT_RECOVER_POINT),
            last_offset: AtomicCell::new(INIT_LAST_OFFSET),
            active_segment_writer,
        })
    }

    /// Opens an existing queue log
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition
    /// * `segments` - Existing read-only segments
    /// * `active_segment` - The active segment for writing
    /// * `log_start_offset` - First valid offset
    /// * `recover_point` - Recovery point offset
    /// * `last_offset` - Last offset in the log
    pub fn open(
        topic_partition: &TopicPartition,
        segments: BTreeMap<i64, ReadOnlySegmentIndex>,
        active_segment: ActiveSegmentIndex,
        log_start_offset: i64,
        recover_point: i64,
        last_offset: i64,
        active_segment_writer: Arc<ActiveSegmentWriter>,
    ) -> AppResult<Self> {
        let segments_order = segments.keys().cloned().collect();
        let active_segment_id = active_segment.base_offset();

        // open active segment writer
        active_segment_writer
            .open_file(topic_partition, active_segment_id)?;

        Ok(Self {
            topic_partition: topic_partition.clone(),
            segments: DashMap::from_iter(segments.into_iter().map(|(k, v)| (k, Arc::new(v)))),
            segments_order: RwLock::new(segments_order),
            active_segment_index: RwLock::new(active_segment),
            active_segment_id: AtomicCell::new(active_segment_id),
            log_start_offset,
            recover_point: AtomicCell::new(recover_point),
            last_offset: AtomicCell::new(last_offset),
            active_segment_writer,
        })
    }

    /// Ensures the directory exists, creating it if necessary
    fn ensure_dir_exists(dir: &str) -> AppResult<()> {
        if !Path::new(dir).exists() {
            info!("log dir does not exists, create queue log dir:{}", dir);
            std::fs::create_dir_all(dir).map_err(|e| {
                AppError::DetailedIoError(format!("create queue log dir: {} error: {}", dir, e))
            })?;
        }
        Ok(())
    }
}
