use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use tracing::{info, warn};

use crate::log::SegmentFileType;
use crate::{
    log::index_file::{ReadOnlyIndexFile, WritableIndexFile},
    log::log_segment::{ActiveLogSegment, LogSegmentCommon, ReadOnlyLogSegment},
    message::TopicPartition,
    AppError, AppResult,
};

use super::QueueLog;

impl QueueLog {
    /// Loads a QueueLog from existing data
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition to load
    /// * `recover_point` - Recovery point offset
    /// * `index_file_max_size` - Maximum size for index files
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

    /// Determines the starting offset for the log
    fn determine_start_offset(
        segments: &BTreeMap<i64, ReadOnlyLogSegment>,
        active_segment: &Option<ActiveLogSegment>,
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

    /// Loads segments from disk
    fn load_segments(
        topic_partition: &TopicPartition,
        index_file_max_size: u32,
    ) -> AppResult<(BTreeMap<i64, ReadOnlyLogSegment>, Option<ActiveLogSegment>)> {
        let dir = topic_partition.partition_dir();
        info!("Loading queue log segments from {}", dir);

        if !Self::has_segment_files(&dir)? {
            return Ok((BTreeMap::new(), None));
        }

        let (index_files, log_files) = Self::scan_segment_files(&dir)?;

        if log_files.is_empty() {
            return Ok((BTreeMap::new(), None));
        }

        Self::build_segments(
            topic_partition,
            &dir,
            index_files,
            log_files,
            index_file_max_size,
        )
    }

    /// Checks if directory has any segment files
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

    /// Parses a segment filename into its components
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

    /// Scans directory for segment files
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

    /// Builds segments from files
    fn build_segments(
        topic_partition: &TopicPartition,
        dir: impl AsRef<Path>,
        index_files: BTreeSet<i64>,
        log_files: BTreeSet<i64>,
        index_file_max_size: u32,
    ) -> AppResult<(BTreeMap<i64, ReadOnlyLogSegment>, Option<ActiveLogSegment>)> {
        let mut segments = BTreeMap::new();
        let mut active_segment = None;

        for base_offset in log_files.iter().rev().copied() {
            if !index_files.contains(&base_offset) {
                continue;
            }

            let index_path = format!("{}/{}.index", dir.as_ref().display(), base_offset);

            if active_segment.is_none() {
                active_segment = Some(Self::create_active_segment(
                    topic_partition,
                    base_offset,
                    &index_path,
                    index_file_max_size,
                )?);
            } else {
                segments.insert(
                    base_offset,
                    Self::create_readonly_segment(topic_partition, base_offset, &index_path)?,
                );
            }
        }

        Ok((segments, active_segment))
    }

    /// Creates an active segment
    fn create_active_segment(
        topic_partition: &TopicPartition,
        base_offset: i64,
        index_path: &str,
        index_file_max_size: u32,
    ) -> AppResult<ActiveLogSegment> {
        let index_file = WritableIndexFile::new(index_path, index_file_max_size as usize)?;
        ActiveLogSegment::open(topic_partition, base_offset, index_file, None)
    }

    /// Creates a readonly segment
    fn create_readonly_segment(
        topic_partition: &TopicPartition,
        base_offset: i64,
        index_path: &str,
    ) -> AppResult<ReadOnlyLogSegment> {
        let index_file = ReadOnlyIndexFile::new(index_path)?;
        Ok(ReadOnlyLogSegment::open(topic_partition, base_offset, index_file))
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
            ActiveLogSegment::new(
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
            ReadOnlyLogSegment::open(
                &TopicPartition::new("test", 0, crate::log::LogType::Queue),
                50,
                ReadOnlyIndexFile::new("dummy").unwrap(),
            ),
        );

        assert_eq!(
            QueueLog::determine_start_offset(&segments, &active_segment),
            50
        );
    }
}
