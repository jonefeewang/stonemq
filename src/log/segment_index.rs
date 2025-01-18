//! Segment Index Implementation
//!
//! This module implements the indexing functionality for log segments, providing efficient
//! offset-based lookup capabilities. It supports both read-only and active (writable) segments
//! with their respective index management strategies.
//!
//! # Index Structure
//!
//! The segment index maintains mappings between:
//! - Logical offsets (message sequence numbers)
//! - Physical positions (file locations)
//!
//! # Components
//!
//! The module provides two main types of indexes:
//! - `ReadOnlySegmentIndex`: For immutable, completed segments
//! - `ActiveSegmentIndex`: For the current, writable segment
//!
//! # Performance
//!
//! Index operations are optimized for:
//! - Fast offset lookup
//! - Efficient index updates
//! - Memory-mapped file access
//! - Concurrent read access

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::log::index_file::{ReadOnlyIndexFile, WritableIndexFile};
use crate::message::TopicPartition;
use crate::{global_config, AppResult};
use tracing::trace;

use super::{LogType, INDEX_FILE_SUFFIX};

/// Common interface for segment index operations.
///
/// This trait defines the core functionality that both read-only
/// and active segment indexes must implement.
pub trait SegmentIndexCommon {
    /// Returns the base offset of the segment.
    fn base_offset(&self) -> i64;

    /// Looks up the position information for a relative offset.
    ///
    /// # Arguments
    ///
    /// * `relative_offset` - Offset relative to the segment's base offset
    ///
    /// # Returns
    ///
    /// * `Option<(u32, u32)>` - Tuple of (offset, position) if found
    fn lookup_index(&self, relative_offset: u32) -> Option<(u32, u32)>;

    /// Gets position information for an absolute offset.
    ///
    /// # Arguments
    ///
    /// * `offset` - Absolute offset to look up
    ///
    /// # Returns
    ///
    /// * `AppResult<PositionInfo>` - Position information if found
    fn get_relative_position(&self, offset: i64) -> AppResult<PositionInfo> {
        let offset_position = self
            .lookup_index((offset - self.base_offset()) as u32)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!(
                        "can not find offset:{} in index file: {}",
                        offset,
                        self.base_offset()
                    ),
                )
            })?;

        Ok(PositionInfo {
            base_offset: self.base_offset(),
            offset: offset_position.0 as i64 + self.base_offset(),
            position: offset_position.1 as i64,
        })
    }
}

/// Read-only segment index for immutable segments.
///
/// Provides efficient lookup operations for completed log segments
/// that will no longer be modified.
#[derive(Debug)]
pub struct ReadOnlySegmentIndex {
    /// Base offset of the segment
    base_offset: i64,
    /// Index file containing offset mappings
    offset_index: ReadOnlyIndexFile,
}

/// Active segment index for the current writable segment.
///
/// Manages index operations for the active segment that is still
/// receiving new messages.
#[derive(Debug)]
pub struct ActiveSegmentIndex {
    /// Base offset of the segment
    base_offset: i64,
    /// Index file for offset mappings
    offset_index: WritableIndexFile,
    /// Bytes written since last index entry
    bytes_since_last_index_entry: AtomicUsize,
}

/// Position information for a specific offset.
///
/// Contains all necessary information to locate a message
/// within a segment file.
#[derive(Debug, Default, Clone, Copy)]
pub struct PositionInfo {
    /// Base offset of the containing segment
    pub base_offset: i64,
    /// Absolute offset of the message
    pub offset: i64,
    /// Physical position in the segment file
    pub position: i64,
}

impl SegmentIndexCommon for ReadOnlySegmentIndex {
    fn base_offset(&self) -> i64 {
        self.base_offset
    }

    fn lookup_index(&self, relative_offset: u32) -> Option<(u32, u32)> {
        self.offset_index.lookup(relative_offset)
    }
}

impl SegmentIndexCommon for ActiveSegmentIndex {
    fn base_offset(&self) -> i64 {
        self.base_offset
    }

    fn lookup_index(&self, relative_offset: u32) -> Option<(u32, u32)> {
        self.offset_index.lookup(relative_offset)
    }
}

impl ReadOnlySegmentIndex {
    /// Opens a new read-only segment index.
    ///
    /// # Arguments
    ///
    /// * `base_offset` - Base offset for the segment
    /// * `offset_index` - Index file to use
    ///
    /// # Returns
    ///
    /// A new ReadOnlySegmentIndex instance
    pub fn open(base_offset: i64, offset_index: ReadOnlyIndexFile) -> Self {
        Self {
            base_offset,
            offset_index,
        }
    }
}

impl ActiveSegmentIndex {
    /// Creates a new active segment index.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - Topic partition this segment belongs to
    /// * `base_offset` - Base offset for the segment
    /// * `index_file_max_size` - Maximum size for the index file
    ///
    /// # Returns
    ///
    /// * `AppResult<Self>` - New active segment index
    pub fn new(
        topic_partition: &TopicPartition,
        base_offset: i64,
        index_file_max_size: usize,
    ) -> AppResult<Self> {
        let index_file_name = format!(
            "{}/{}.{}",
            topic_partition.partition_dir(),
            base_offset,
            INDEX_FILE_SUFFIX
        );
        let offset_index = WritableIndexFile::new(index_file_name, index_file_max_size)?;
        Self::open(base_offset, offset_index, None)
    }

    /// Opens an existing active segment index.
    ///
    /// # Arguments
    ///
    /// * `base_offset` - Base offset for the segment
    /// * `offset_index` - Index file to use
    /// * `_time_index` - Optional time-based index file
    ///
    /// # Returns
    ///
    /// * `AppResult<Self>` - Opened active segment index
    pub fn open(
        base_offset: i64,
        offset_index: WritableIndexFile,
        _time_index: Option<WritableIndexFile>,
    ) -> AppResult<Self> {
        Ok(Self {
            base_offset,
            offset_index,
            bytes_since_last_index_entry: AtomicUsize::new(0),
        })
    }

    /// Updates the index with a new entry.
    ///
    /// # Arguments
    ///
    /// * `offset` - Absolute offset to index
    /// * `records_size` - Size of the records being indexed
    /// * `log_type` - Type of log (Journal or Queue)
    /// * `segment_size` - Current size of the segment
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - Success if index is updated
    pub fn update_index(
        &mut self,
        offset: i64,
        records_size: usize,
        log_type: LogType,
        segment_size: u64,
    ) -> AppResult<()> {
        let relative_offset = offset - self.base_offset;

        let index_interval = match log_type {
            LogType::Journal => global_config().log.journal_index_interval_bytes,
            LogType::Queue => global_config().log.queue_index_interval_bytes,
        };

        if index_interval <= self.bytes_since_last_index_entry.load(Ordering::Acquire) {
            trace!("add_entry");
            self.offset_index
                .add_entry(relative_offset as u32, segment_size as u32)?;

            trace!(
                "write index entry: {},{},{:?},{},{}",
                relative_offset,
                segment_size,
                self.offset_index,
                index_interval,
                self.bytes_since_last_index_entry.load(Ordering::Acquire)
            );

            self.bytes_since_last_index_entry
                .store(0, Ordering::Release);
        }
        self.bytes_since_last_index_entry
            .fetch_add(records_size, Ordering::AcqRel);

        Ok(())
    }

    /// Checks if the offset index is full.
    ///
    /// # Returns
    ///
    /// `true` if the index has reached its maximum size
    pub fn offset_index_full(&self) -> bool {
        self.offset_index.is_full()
    }

    /// Converts this active segment index to a read-only index.
    ///
    /// # Returns
    ///
    /// * `AppResult<ReadOnlySegmentIndex>` - New read-only index
    pub fn into_readonly(self) -> AppResult<ReadOnlySegmentIndex> {
        let readonly_offset_index = self.offset_index.into_readonly()?;

        Ok(ReadOnlySegmentIndex {
            base_offset: self.base_offset,
            offset_index: readonly_offset_index,
        })
    }

    /// Flushes index changes to disk.
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - Success if flush completes
    pub fn flush_index(&mut self) -> AppResult<()> {
        self.offset_index.flush()?;
        Ok(())
    }

    /// Closes the index, ensuring all changes are written.
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - Success if close completes
    pub fn close(&mut self) -> AppResult<()> {
        self.offset_index.close()?;
        Ok(())
    }
}
