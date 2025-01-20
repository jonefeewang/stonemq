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

//! Queue Log Loading Implementation
//!
//! This module handles the loading and initialization of queue logs from disk.
//! It provides functionality for:
//! - Loading existing log segments
//! - Recovering log state after crashes
//! - Initializing new logs when none exist
//!
//! # Loading Process
//!
//! The loading process follows these steps:
//! 1. Scan directory for segment files
//! 2. Parse segment filenames to extract offsets
//! 3. Load index files for segments
//! 4. Initialize active and read-only segments
//! 5. Reconstruct log state from loaded segments
//!
//! # File Structure
//!
//! Queue logs use several file types:
//! - `.log`: Contains the actual message data
//! - `.index`: Contains offset indexes for message lookup
//! - `.time_index`: Contains time-based indexes (optional)

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use tracing::{info, warn};

use crate::log::SegmentFileType;
use crate::{
    log::index_file::{ReadOnlyIndexFile, WritableIndexFile},
    log::segment_index::{ActiveSegmentIndex, ReadOnlySegmentIndex, SegmentIndexCommon},
    message::TopicPartition,
    AppError, AppResult,
};

use super::QueueLog;

impl QueueLog {
    /// Loads a QueueLog from existing data on disk.
    ///
    /// This method handles the complete process of loading a queue log,
    /// including scanning for segments, initializing indexes, and setting
    /// up the active segment.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition to load
    /// * `recover_point` - Offset to start recovery from
    /// * `index_file_max_size` - Maximum size for index files
    ///
    /// # Returns
    ///
    /// * `AppResult<Self>` - Loaded queue log or error
    ///
    /// # Recovery Process
    ///
    /// 1. Load existing segments from disk
    /// 2. Initialize active segment
    /// 3. Determine starting offset
    /// 4. Set up recovery point
    pub fn load_from(
        topic_partition: &TopicPartition,
        recover_point: i64,
        index_file_max_size: u32,
    ) -> AppResult<Self> {
        let (segments, active_segment) = Self::load_segments(topic_partition, index_file_max_size)?;

        if active_segment.is_none() {
            let log = Self::new(topic_partition)?;
            return Ok(log);
        }

        let log_start_offset = Self::determine_start_offset(&segments, &active_segment);

        let log = QueueLog::open(
            topic_partition,
            segments,
            active_segment.unwrap(),
            log_start_offset,
            recover_point,
            recover_point,
        )?;

        Ok(log)
    }

    /// Determines the starting offset for the log.
    ///
    /// The starting offset is determined by:
    /// - First segment's base offset if segments exist
    /// - Active segment's base offset if no other segments
    /// - 0 if no segments exist at all
    ///
    /// # Arguments
    ///
    /// * `segments` - Map of existing read-only segments
    /// * `active_segment` - Optional active segment
    ///
    /// # Returns
    ///
    /// The determined starting offset
    fn determine_start_offset(
        segments: &BTreeMap<i64, ReadOnlySegmentIndex>,
        active_segment: &Option<ActiveSegmentIndex>,
    ) -> i64 {
        if segments.is_empty() {
            active_segment
                .as_ref()
                .map(|s| s.base_offset())
                .unwrap_or(0)
        } else {
            *segments.first_key_value().unwrap().0
        }
    }

    /// Loads all segments from disk for a topic partition.
    ///
    /// Scans the directory and loads both read-only segments and
    /// initializes the active segment.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - Topic partition to load segments for
    /// * `index_file_max_size` - Maximum size for index files
    ///
    /// # Returns
    ///
    /// * `AppResult<(BTreeMap<i64, ReadOnlySegmentIndex>, Option<ActiveSegmentIndex>)>` -
    ///   Tuple of read-only segments and optional active segment
    fn load_segments(
        topic_partition: &TopicPartition,
        index_file_max_size: u32,
    ) -> AppResult<(
        BTreeMap<i64, ReadOnlySegmentIndex>,
        Option<ActiveSegmentIndex>,
    )> {
        let dir = topic_partition.partition_dir();
        info!("Loading queue log segments from {}", dir);

        if !Self::has_segment_files(&dir)? {
            return Ok((BTreeMap::new(), None));
        }

        let (index_files, log_files) = Self::scan_segment_files(&dir)?;

        if log_files.is_empty() {
            return Ok((BTreeMap::new(), None));
        }

        Self::build_segments(&dir, index_files, log_files, index_file_max_size)
    }

    /// Checks if a directory contains any segment files.
    ///
    /// # Arguments
    ///
    /// * `dir` - Directory path to check
    ///
    /// # Returns
    ///
    /// * `AppResult<bool>` - True if directory contains files, false otherwise
    fn has_segment_files(dir: impl AsRef<Path>) -> AppResult<bool> {
        Ok(std::fs::read_dir(&dir)
            .map_err(|e| {
                AppError::DetailedIoError(format!(
                    "Failed to read directory {}: {}",
                    dir.as_ref().display(),
                    e
                ))
            })?
            .next()
            .is_some())
    }

    /// Parses a segment filename to extract base offset and file type.
    ///
    /// Expects filenames in format: `<base_offset>.<extension>`
    /// where extension is one of:
    /// - "index" for index files
    /// - "log" for log files
    /// - "time_index" for time index files
    ///
    /// # Arguments
    ///
    /// * `filename` - Name of the file to parse
    ///
    /// # Returns
    ///
    /// * `Option<(i64, SegmentFileType)>` - Base offset and file type if valid
    fn parse_segment_filename(filename: &str) -> Option<(i64, SegmentFileType)> {
        let (base, ext) = filename.rsplit_once('.')?;
        let base_offset = base.parse::<i64>().ok()?;

        let file_type = match ext {
            "index" => SegmentFileType::Index,
            "log" => SegmentFileType::Log,
            "time_index" => SegmentFileType::TimeIndex,
            _ => SegmentFileType::Unknown,
        };

        Some((base_offset, file_type))
    }

    /// Scans a directory for segment files and categorizes them.
    ///
    /// Creates sets of base offsets for:
    /// - Index files
    /// - Log files
    ///
    /// # Arguments
    ///
    /// * `dir` - Directory to scan
    ///
    /// # Returns
    ///
    /// * `AppResult<(BTreeSet<i64>, BTreeSet<i64>)>` - Sets of index and log file offsets
    fn scan_segment_files(dir: impl AsRef<Path>) -> AppResult<(BTreeSet<i64>, BTreeSet<i64>)> {
        let mut index_files = BTreeSet::new();
        let mut log_files = BTreeSet::new();

        for entry in std::fs::read_dir(dir.as_ref()).map_err(|e| {
            AppError::DetailedIoError(format!(
                "Failed to read directory {}: {}",
                dir.as_ref().display(),
                e
            ))
        })? {
            let entry = entry.map_err(|e| {
                AppError::DetailedIoError(format!("Failed to read directory entry: {}", e))
            })?;

            if !entry
                .file_type()
                .map_err(|e| AppError::DetailedIoError(format!("Failed to get file type: {}", e)))?
                .is_file()
            {
                continue;
            }

            let filename = entry.file_name().to_string_lossy().to_string();

            if let Some((base_offset, file_type)) = Self::parse_segment_filename(&filename) {
                match file_type {
                    SegmentFileType::Index => index_files.insert(base_offset),
                    SegmentFileType::Log => log_files.insert(base_offset),
                    SegmentFileType::TimeIndex => continue,
                    SegmentFileType::Unknown => {
                        warn!("Invalid segment file name: {}", filename);
                        continue;
                    }
                };
            }
        }

        Ok((index_files, log_files))
    }

    /// Builds segment objects from scanned files.
    ///
    /// Creates:
    /// - Read-only segments for all but the latest offset
    /// - Active segment for the latest offset
    ///
    /// # Arguments
    ///
    /// * `dir` - Base directory for segments
    /// * `index_files` - Set of index file offsets
    /// * `log_files` - Set of log file offsets
    /// * `index_file_max_size` - Maximum size for index files
    ///
    /// # Returns
    ///
    /// * `AppResult<(BTreeMap<i64, ReadOnlySegmentIndex>, Option<ActiveSegmentIndex>)>` -
    ///   Map of read-only segments and optional active segment
    fn build_segments(
        dir: impl AsRef<Path>,
        index_files: BTreeSet<i64>,
        log_files: BTreeSet<i64>,
        index_file_max_size: u32,
    ) -> AppResult<(
        BTreeMap<i64, ReadOnlySegmentIndex>,
        Option<ActiveSegmentIndex>,
    )> {
        let mut segments = BTreeMap::new();
        let mut active_segment = None;

        for base_offset in log_files.iter().rev().copied() {
            if !index_files.contains(&base_offset) {
                continue;
            }

            let index_path = format!("{}/{}.index", dir.as_ref().display(), base_offset);

            if active_segment.is_none() {
                active_segment = Some(Self::create_active_segment(
                    base_offset,
                    &index_path,
                    index_file_max_size,
                )?);
            } else {
                segments.insert(
                    base_offset,
                    Self::create_readonly_segment(base_offset, &index_path)?,
                );
            }
        }

        Ok((segments, active_segment))
    }

    /// Creates an active segment for writing.
    ///
    /// # Arguments
    ///
    /// * `base_offset` - Base offset for the segment
    /// * `index_path` - Path to the index file
    /// * `index_file_max_size` - Maximum size for the index file
    ///
    /// # Returns
    ///
    /// * `AppResult<ActiveSegmentIndex>` - New active segment
    fn create_active_segment(
        base_offset: i64,
        index_path: &str,
        index_file_max_size: u32,
    ) -> AppResult<ActiveSegmentIndex> {
        let index_file = WritableIndexFile::new(index_path, index_file_max_size as usize)?;
        ActiveSegmentIndex::open(base_offset, index_file, None)
    }

    /// Creates a read-only segment for completed segments.
    ///
    /// # Arguments
    ///
    /// * `base_offset` - Base offset for the segment
    /// * `index_path` - Path to the index file
    ///
    /// # Returns
    ///
    /// * `AppResult<ReadOnlySegmentIndex>` - New read-only segment
    fn create_readonly_segment(
        base_offset: i64,
        index_path: &str,
    ) -> AppResult<ReadOnlySegmentIndex> {
        let index_file = ReadOnlyIndexFile::new(index_path)?;
        Ok(ReadOnlySegmentIndex::open(base_offset, index_file))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_segment_filename() {
        assert_eq!(
            QueueLog::parse_segment_filename("100.index"),
            Some((100, SegmentFileType::Index))
        );
        assert_eq!(
            QueueLog::parse_segment_filename("100.log"),
            Some((100, SegmentFileType::Log))
        );
        assert_eq!(QueueLog::parse_segment_filename("invalid"), None);
    }

    #[test]
    fn test_determine_start_offset() {
        let mut segments = BTreeMap::new();
        let active_segment = Some(
            ActiveSegmentIndex::new(
                &TopicPartition::new("test", 0, crate::log::LogType::Queue),
                100,
                1024,
            )
            .unwrap(),
        );

        assert_eq!(
            QueueLog::determine_start_offset(&segments, &active_segment),
            100
        );

        segments.insert(
            50,
            ReadOnlySegmentIndex::open(50, ReadOnlyIndexFile::new("dummy").unwrap()),
        );

        assert_eq!(
            QueueLog::determine_start_offset(&segments, &active_segment),
            50
        );
    }
}
