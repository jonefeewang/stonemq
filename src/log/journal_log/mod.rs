// Copyright 2025 jonefeewang@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    sync::{atomic::AtomicI64, Arc},
};

use dashmap::DashMap;
use parking_lot::RwLock;

use crate::{
    global_config,
    log::{CheckPointFile, NEXT_OFFSET_CHECKPOINT_FILE_NAME},
    message::TopicPartition,
    AppError, AppResult,
};

use super::{
    get_active_segment_writer,
    segment_index::{ActiveSegmentIndex, ReadOnlySegmentIndex, SegmentIndexCommon},
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
    active_segment_base_offset: AtomicI64,

    /// Next offset information for queues
    queue_next_offset_info: DashMap<TopicPartition, i64>,

    /// Starting offset of the log
    _log_start_offset: AtomicI64,

    /// Next offset to be assigned
    pub(crate) next_offset: AtomicI64,

    /// Recovery point offset
    pub(crate) recover_point: AtomicI64,

    /// Split point offset
    pub split_offset: AtomicI64,

    /// Topic partition information
    topic_partition: TopicPartition,

    /// Checkpoint file for queue next offsets
    queue_next_offset_checkpoints: CheckPointFile,
    // active_segment_writer: Arc<ActiveSegmentWriter>,
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
    pub fn new(topic_partition: &TopicPartition) -> AppResult<Self> {
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
        get_active_segment_writer()
            .open_file(topic_partition, active_segment_index.base_offset())?;

        let segments_order = BTreeSet::from([active_segment_index.base_offset()]);

        Ok(Self {
            segments_order: RwLock::new(segments_order),
            segment_index: DashMap::new(),
            active_segment_index: RwLock::new(active_segment_index),
            active_segment_base_offset: AtomicI64::new(0),
            queue_next_offset_info: DashMap::new(),
            queue_next_offset_checkpoints: CheckPointFile::new(format!(
                "{}/{}",
                &topic_partition.partition_dir(),
                NEXT_OFFSET_CHECKPOINT_FILE_NAME
            )),
            _log_start_offset: AtomicI64::new(Self::INIT_LOG_START_OFFSET),
            next_offset: AtomicI64::new(Self::INIT_NEXT_OFFSET),
            recover_point: AtomicI64::new(Self::INIT_RECOVER_POINT),
            split_offset: AtomicI64::new(Self::INIT_SPLIT_OFFSET),
            topic_partition: topic_partition.clone(),
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
    ) -> AppResult<Self> {
        let mut segments_order = segments.keys().cloned().collect::<BTreeSet<i64>>();
        let segments_map = segments
            .into_iter()
            .map(|(k, v)| (k, Arc::new(v)))
            .collect();

        let active_segment_base_offset = active_segment.base_offset();
        segments_order.insert(active_segment_base_offset);

        // open active segment writer
        get_active_segment_writer().open_file(topic_partition, active_segment_base_offset)?;

        Ok(Self {
            segments_order: RwLock::new(segments_order),
            segment_index: segments_map,
            active_segment_index: RwLock::new(active_segment),
            active_segment_base_offset: AtomicI64::new(active_segment_base_offset),
            queue_next_offset_info: metadata.queue_next_offset_info,
            queue_next_offset_checkpoints: metadata.queue_next_offset_checkpoints,
            _log_start_offset: AtomicI64::new(metadata.log_start_offset),
            next_offset: AtomicI64::new(metadata.next_offset),
            recover_point: AtomicI64::new(metadata.recover_point),
            split_offset: AtomicI64::new(metadata.split_offset),
            topic_partition: topic_partition.clone(),
        })
    }
}
