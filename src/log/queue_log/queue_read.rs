//! Queue Log Reading Implementation
//!
//! This module implements the reading functionality for queue logs. It provides
//! methods for reading records from log segments, handling offsets, and managing
//! read positions.
//!
//! # Reading Process
//!
//! The reading process involves several steps:
//! 1. Finding the correct segment for a given offset
//! 2. Seeking to the correct position within the segment
//! 3. Reading records up to the requested size
//! 4. Handling segment boundaries and transitions
//!
//! # Position Management
//!
//! The module maintains several types of positions:
//! - Physical file positions
//! - Logical offsets
//! - Segment base offsets
//!
//! # Performance Considerations
//!
//! Reading is optimized through:
//! - Buffered reading
//! - Position caching
//! - Efficient segment lookup

use std::io::Read;
use std::path::PathBuf;
use std::{fs::File, sync::atomic::Ordering};

use bytes::BytesMut;
use tracing::{debug, trace};

use crate::{
    log::{
        get_active_segment_writer, seek_file, segment_index::SegmentIndexCommon, LogType,
        PositionInfo, NO_POSITION_INFO,
    },
    message::{LogFetchInfo, MemoryRecords, TopicPartition},
    AppError, AppResult,
};

use super::QueueLog;

/// Configuration for reading records from a queue log.
///
/// Controls various aspects of the read operation including:
/// - Maximum position to read to
/// - Target offset position to start from
/// - Maximum size of data to read
#[derive(Debug, Clone)]
struct ReadConfig {
    /// Maximum position that can be read up to
    max_position: u64,
    /// Target position to start reading from
    target_offset_position: u64,
    /// Maximum size of data to read in bytes
    max_size: i32,
}

impl QueueLog {
    /// Gets the current recovery point offset.
    ///
    /// The recovery point represents the last known good offset
    /// that has been fully written and can be safely read.
    ///
    /// # Returns
    ///
    /// The current recovery point offset
    pub fn get_recover_point(&self) -> i64 {
        self.recover_point.load(Ordering::Acquire)
    }

    /// Gets the Log End Offset (LEO) information.
    ///
    /// Returns information about the current end of the log including:
    /// - Base offset of the active segment
    /// - Last offset in the log
    /// - Physical position in the active segment
    ///
    /// # Returns
    ///
    /// * `AppResult<PositionInfo>` - Position information for the log end
    pub fn get_leo_info(&self) -> AppResult<PositionInfo> {
        let active_seg_size =
            get_active_segment_writer().active_segment_size(&self.topic_partition);

        Ok(PositionInfo {
            base_offset: self.active_segment_id.load(Ordering::Acquire),
            offset: self.last_offset.load(Ordering::Acquire),
            position: active_seg_size as i64,
        })
    }

    /// Gets position information for a specific offset.
    ///
    /// Finds the segment containing the offset and returns its position information.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset to get position information for
    ///
    /// # Returns
    ///
    /// * `AppResult<PositionInfo>` - Position information for the offset
    pub fn get_reference_position_info(&self, offset: i64) -> AppResult<PositionInfo> {
        debug!("get_reference_position_info: {}", offset);
        let base_offset = self.find_segment_for_offset(offset)?;
        debug!("get_reference_position_info: {}", base_offset);
        self.get_segment_position(base_offset, offset)
    }

    /// Finds the appropriate segment for a given offset.
    ///
    /// Searches through segments in reverse order to find the segment
    /// containing the specified offset.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset to find a segment for
    ///
    /// # Returns
    ///
    /// * `AppResult<i64>` - Base offset of the containing segment
    fn find_segment_for_offset(&self, offset: i64) -> AppResult<i64> {
        self.segments_order
            .read()
            .iter()
            .rev()
            .find(|&&seg_offset| seg_offset <= offset)
            .copied()
            .ok_or_else(|| {
                AppError::InvalidValue(format!("no segment found for offset {}", offset))
            })
    }

    /// Gets position information within a specific segment.
    ///
    /// Handles both active and read-only segments to find the exact
    /// position for a given offset.
    ///
    /// # Arguments
    ///
    /// * `base_offset` - Base offset of the segment
    /// * `offset` - Target offset to get position for
    ///
    /// # Returns
    ///
    /// * `AppResult<PositionInfo>` - Position information within the segment
    fn get_segment_position(&self, base_offset: i64, offset: i64) -> AppResult<PositionInfo> {
        debug!("get_segment_position: {} {}", base_offset, offset);
        if base_offset == self.active_segment_id.load(Ordering::Acquire) {
            self.active_segment_index
                .read()
                .get_relative_position(offset)
        } else {
            self.segments
                .get(&base_offset)
                .ok_or_else(|| {
                    AppError::InvalidValue(format!("segment not found for offset {}", offset))
                })?
                .get_relative_position(offset)
        }
    }

    /// Reads records from the log starting at a specific offset.
    ///
    /// This is the main entry point for reading records. It handles:
    /// 1. Finding the correct segment
    /// 2. Seeking to the right position
    /// 3. Reading up to the specified size
    /// 4. Handling end of segments
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition to read from
    /// * `start_offset` - Offset to start reading from
    /// * `max_size` - Maximum number of bytes to read
    ///
    /// # Returns
    ///
    /// * `AppResult<LogFetchInfo>` - Fetched records and metadata
    pub async fn read_records(
        &self,
        topic_partition: &TopicPartition,
        start_offset: i64,
        max_size: i32,
    ) -> AppResult<LogFetchInfo> {
        trace!(
            "Reading records from partition: {}, start_offset: {}, max_size: {}",
            topic_partition.id(),
            start_offset,
            max_size
        );

        // retrieve the segment information where the `start_offset` resides.
        let ref_position_info = match self.get_reference_position_info(start_offset) {
            Ok(info) => info,
            Err(_) => return Ok(self.create_empty_fetch_info()),
        };

        debug!("ref_position_info: {:?}", ref_position_info);

        // open the segment file where the `start_offset` resides.
        let segment_file = self.open_segment_file(topic_partition, &ref_position_info)?;
        debug!("open segment file: {:?}", segment_file);

        // seek file to the target position
        let (file, target_position_info) = match self
            .seek_to_position(segment_file, start_offset, ref_position_info)
            .await
        {
            Ok(result) => result,
            Err(e) => {
                debug!("seek to position failed: {}", e);
                return Ok(self.create_empty_fetch_info());
            }
        };

        debug!("target_position_info: {:?}", target_position_info);

        // calculate read config
        let read_config = self.calculate_read_config(
            &ref_position_info,
            target_position_info.position as u64,
            max_size,
        )?;

        debug!("read_config: {:?}", read_config);

        // do read records
        self.do_read_records(file, topic_partition, read_config, target_position_info)
            .await
    }

    /// Opens a segment file for reading.
    ///
    /// Creates a file handle for the segment containing the specified position.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - Topic partition of the segment
    /// * `position_info` - Position information for the segment
    ///
    /// # Returns
    ///
    /// * `AppResult<File>` - Opened file handle
    fn open_segment_file(
        &self,
        topic_partition: &TopicPartition,
        position_info: &PositionInfo,
    ) -> AppResult<File> {
        let segment_path = PathBuf::from(topic_partition.partition_dir())
            .join(format!("{}.log", position_info.base_offset));
        debug!("open segment file: {}", segment_path.display());
        File::open(&segment_path).map_err(|e| {
            AppError::DetailedIoError(format!(
                "Failed to open segment file: {} error: {}",
                segment_path.display(),
                e
            ))
        })
    }

    /// Seeks to a specific position within a segment file.
    ///
    /// Positions the file cursor at the correct location for reading.
    ///
    /// # Arguments
    ///
    /// * `file` - File handle to seek within
    /// * `start_offset` - Target offset to seek to
    /// * `position_info` - Position information for seeking
    ///
    /// # Returns
    ///
    /// * `AppResult<(File, PositionInfo)>` - Updated file and position
    async fn seek_to_position(
        &self,
        file: File,
        start_offset: i64,
        position_info: PositionInfo,
    ) -> AppResult<(File, PositionInfo)> {
        seek_file(file, start_offset, position_info, LogType::Queue)
            .await
            .map_err(|e| AppError::DetailedIoError(format!("Failed to seek to position: {}", e)))
    }

    /// Calculates configuration for a read operation.
    ///
    /// Determines read boundaries and limits based on:
    /// - Segment boundaries
    /// - Maximum read size
    /// - Current position
    ///
    /// # Arguments
    ///
    /// * `position_info` - Current position information
    /// * `target_position` - Target position to read from
    /// * `max_size` - Maximum size to read
    ///
    /// # Returns
    ///
    /// * `AppResult<ReadConfig>` - Calculated read configuration
    fn calculate_read_config(
        &self,
        position_info: &PositionInfo,
        target_position: u64,
        max_size: i32,
    ) -> AppResult<ReadConfig> {
        let max_position = self.calc_max_read_size(position_info.base_offset) as u64;

        Ok(ReadConfig {
            max_position,
            target_offset_position: target_position,
            max_size,
        })
    }

    /// Calculates the maximum readable size for a segment.
    ///
    /// Handles different size calculations for:
    /// - Active segments (up to current write position)
    /// - Read-only segments (entire segment size)
    ///
    /// # Arguments
    ///
    /// * `base_offset` - Base offset of the segment
    ///
    /// # Returns
    ///
    /// Maximum number of bytes that can be read
    fn calc_max_read_size(&self, base_offset: i64) -> usize {
        let segments = self.segments_order.read();
        segments
            .range(..=base_offset)
            .next_back()
            .copied()
            .map(|offset| {
                if offset == self.active_segment_id.load(Ordering::Acquire) {
                    get_active_segment_writer().readable_size(&self.topic_partition) as usize
                } else {
                    self.readonly_segment_size(offset)
                }
            })
            .unwrap()
    }

    /// Performs the actual reading of records from a file.
    ///
    /// Handles the low-level reading operation including:
    /// - Respecting size limits
    /// - Creating record batches
    /// - Collecting metadata
    ///
    /// # Arguments
    ///
    /// * `file` - File to read from
    /// * `topic_partition` - Topic partition being read
    /// * `config` - Read configuration
    /// * `target_position_info` - Target position information
    ///
    /// # Returns
    ///
    /// * `AppResult<LogFetchInfo>` - Read records and metadata
    async fn do_read_records(
        &self,
        file: File,
        topic_partition: &TopicPartition,
        config: ReadConfig,
        target_position_info: PositionInfo,
    ) -> AppResult<LogFetchInfo> {
        let ReadConfig {
            max_position,
            target_offset_position,
            max_size,
        } = config;

        let left_len = max_position - target_offset_position;

        debug!(
            "Reading from partition: {}, remaining length: {}",
            topic_partition.id(),
            left_len
        );

        if left_len == 0 {
            return Ok(self.create_empty_fetch_info());
        }

        let read_size = if left_len < max_size as u64 {
            left_len as usize
        } else {
            max_size as usize
        };

        let buffer = self.read_file_chunk(file, read_size).await?;
        let records = MemoryRecords::new(buffer);

        trace!(
            "First batch base offset: {:?}",
            records.first_batch_base_offset()
        );

        Ok(LogFetchInfo {
            records,
            log_start_offset: self.log_start_offset,
            log_end_offset: self.last_offset.load(Ordering::Acquire),
            position_info: target_position_info,
        })
    }

    /// Reads a chunk of data from a file.
    ///
    /// Performs the actual I/O operation to read bytes from disk.
    ///
    /// # Arguments
    ///
    /// * `file` - File to read from
    /// * `size` - Number of bytes to read
    ///
    /// # Returns
    ///
    /// * `AppResult<BytesMut>` - Read bytes
    async fn read_file_chunk(&self, mut file: File, size: usize) -> AppResult<BytesMut> {
        tokio::task::spawn_blocking(move || {
            let mut buffer = BytesMut::zeroed(size);
            file.read_exact(&mut buffer)?;
            Ok(buffer)
        })
        .await
        .map_err(|e| AppError::DetailedIoError(format!("Failed to read file: {}", e)))?
    }

    /// create empty fetch info
    pub fn create_empty_fetch_info(&self) -> LogFetchInfo {
        LogFetchInfo {
            records: MemoryRecords::empty(),
            log_start_offset: self.log_start_offset,
            log_end_offset: self.last_offset.load(Ordering::Acquire),
            position_info: NO_POSITION_INFO,
        }
    }

    /// calculate queue log segment size
    pub fn readonly_segment_size(&self, base_offset: i64) -> usize {
        let segment_path = PathBuf::from(self.topic_partition.partition_dir())
            .join(format!("{}.log", base_offset));
        match File::open(&segment_path) {
            Ok(file) => match file.metadata() {
                Ok(metadata) => metadata.len() as usize,
                Err(_) => 0,
            },
            Err(_) => 0,
        }
    }
}
