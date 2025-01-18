//! Queue Log Implementation
//!
//! This module implements a queue-based log storage system that manages message records
//! in segments. It provides efficient storage and retrieval of messages while maintaining
//! ordering and offset information.
//!
//! # Architecture
//!
//! The queue log system is built around these key concepts:
//! - Segments: Fixed-size portions of the log that contain message records
//! - Active Segment: The current segment being written to
//! - Read-only Segments: Previously filled segments that are now immutable
//! - Offsets: Positions in the log used for message tracking and recovery
//!
//! # Components
//!
//! - `queue_load.rs`: Handles loading and recovery of queue logs from disk
//! - `queue_read.rs`: Implements reading operations from queue segments
//! - `queue_write.rs`: Manages write operations and segment rolling
//!
//! # Features
//!
//! - Thread-safe concurrent access to log segments
//! - Automatic segment rolling when size limits are reached
//! - Recovery point tracking for crash recovery
//! - Efficient offset-based message lookup
//! - Atomic operations for consistency

mod queue_load;
mod queue_read;
mod queue_write;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{atomic::AtomicI64, Arc},
};

use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::info;

use crate::{
    global_config,
    log::segment_index::{ActiveSegmentIndex, ReadOnlySegmentIndex, SegmentIndexCommon},
    message::TopicPartition,
    AppError, AppResult,
};

use super::get_active_segment_writer;

/// Constants defining initial values for queue log operations
///
/// These values are used when creating new queue logs or resetting state:
/// - `INIT_LOG_START_OFFSET`: Initial offset for new logs
/// - `INIT_RECOVER_POINT`: Initial recovery point marker
/// - `INIT_LAST_OFFSET`: Initial last offset value
const INIT_LOG_START_OFFSET: i64 = 0;
const INIT_RECOVER_POINT: i64 = -1;
const INIT_LAST_OFFSET: i64 = 0;

/// Queue log manager for a single topic partition.
///
/// Manages the storage and retrieval of messages in a segmented log structure.
/// Provides thread-safe access to log segments and maintains consistency through
/// atomic operations and locks.
///
/// # Thread Safety
///
/// The struct uses a combination of atomic values and locks to ensure thread safety:
/// - `DashMap` for concurrent segment access
/// - `RwLock` for segment ordering and active segment management
/// - `AtomicI64` for offset tracking
///
/// # State Management
///
/// Maintains several types of state:
/// - Segment state (active and read-only segments)
/// - Offset tracking (start, last, recovery points)
/// - Segment ordering for sequential access
#[derive(Debug)]
pub struct QueueLog {
    /// Map of segment base offsets to read-only segments.
    /// Uses DashMap for concurrent access to multiple segments.
    segments: DashMap<i64, Arc<ReadOnlySegmentIndex>>,

    /// Ordered set of segment base offsets.
    /// Protected by RwLock since modifications are rare (only during segment rolling).
    segments_order: RwLock<BTreeSet<i64>>,

    /// Currently active segment for writing new messages.
    /// Protected by RwLock to ensure exclusive access during writes.
    active_segment_index: RwLock<ActiveSegmentIndex>,

    /// Base offset of the current active segment.
    /// Atomic for safe concurrent access.
    active_segment_id: AtomicI64,

    /// Topic partition this log belongs to.
    /// Immutable after construction.
    topic_partition: TopicPartition,

    /// First valid offset in the log.
    /// Used for log cleaning and retention.
    log_start_offset: i64,

    /// Last known valid offset for recovery.
    /// Atomic for safe concurrent updates.
    recover_point: AtomicI64,

    /// Last offset in the log.
    /// Atomic for safe concurrent updates.
    last_offset: AtomicI64,
}

impl QueueLog {
    /// Creates a new empty queue log for a topic partition.
    ///
    /// Initializes a new log with:
    /// - Empty segment map
    /// - Single active segment
    /// - Default offset values
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition this log belongs to
    ///
    /// # Returns
    ///
    /// * `AppResult<Self>` - New QueueLog instance or error if creation fails
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Directory creation fails
    /// - Active segment initialization fails
    /// - Segment writer initialization fails
    pub fn new(topic_partition: &TopicPartition) -> AppResult<Self> {
        let dir = topic_partition.partition_dir();
        Self::ensure_dir_exists(&dir)?;

        let index_file_max_size = global_config().log.queue_index_file_size;
        let active_segment_index =
            ActiveSegmentIndex::new(topic_partition, 0, index_file_max_size)?;

        // open active segment writer
        get_active_segment_writer()
            .open_file(topic_partition, active_segment_index.base_offset())?;
        let segments_order = BTreeSet::from([active_segment_index.base_offset()]);

        Ok(Self {
            topic_partition: topic_partition.clone(),
            segments: DashMap::with_capacity(1),
            segments_order: RwLock::new(segments_order),
            active_segment_index: RwLock::new(active_segment_index),
            active_segment_id: AtomicI64::new(0),
            log_start_offset: INIT_LOG_START_OFFSET,
            recover_point: AtomicI64::new(INIT_RECOVER_POINT),
            last_offset: AtomicI64::new(INIT_LAST_OFFSET),
        })
    }

    /// Opens an existing queue log with the provided state.
    ///
    /// Reconstructs a queue log from:
    /// - Existing read-only segments
    /// - Active segment state
    /// - Stored offset information
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition
    /// * `segments` - Map of existing read-only segments
    /// * `active_segment` - The active segment for writing
    /// * `log_start_offset` - First valid offset in the log
    /// * `recover_point` - Last known valid offset for recovery
    /// * `last_offset` - Last offset in the log
    ///
    /// # Returns
    ///
    /// * `AppResult<Self>` - Reconstructed QueueLog instance or error
    pub fn open(
        topic_partition: &TopicPartition,
        segments: BTreeMap<i64, ReadOnlySegmentIndex>,
        active_segment: ActiveSegmentIndex,
        log_start_offset: i64,
        recover_point: i64,
        last_offset: i64,
    ) -> AppResult<Self> {
        let mut segments_order = segments.keys().cloned().collect::<BTreeSet<i64>>();
        let active_segment_base_offset = active_segment.base_offset();

        // open active segment writer
        get_active_segment_writer().open_file(topic_partition, active_segment_base_offset)?;
        segments_order.insert(active_segment_base_offset);

        Ok(Self {
            topic_partition: topic_partition.clone(),
            segments: DashMap::from_iter(segments.into_iter().map(|(k, v)| (k, Arc::new(v)))),
            segments_order: RwLock::new(segments_order),
            active_segment_index: RwLock::new(active_segment),
            active_segment_id: AtomicI64::new(active_segment_base_offset),
            log_start_offset,
            recover_point: AtomicI64::new(recover_point),
            last_offset: AtomicI64::new(last_offset),
        })
    }

    /// Ensures the log directory exists, creating it if necessary.
    ///
    /// # Arguments
    ///
    /// * `dir` - Directory path to check/create
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - Success if directory exists or is created
    ///
    /// # Errors
    ///
    /// Returns error if directory creation fails
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
