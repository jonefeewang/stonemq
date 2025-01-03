use dashmap::DashMap;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};
use tracing::{info, trace};

use super::{JournalLog, JournalLogMetadata};
use crate::{
    log::{
        index_file::{ReadOnlyIndexFile, WritableIndexFile},
        segment_index::{ActiveSegmentIndex, ReadOnlySegmentIndex, SegmentIndexCommon},
        CheckPointFile, LogType, SegmentFileType, NEXT_OFFSET_CHECKPOINT_FILE_NAME,
    },
    message::TopicPartition,
    AppError, AppResult,
};

impl JournalLog {
    /// Loads a journal log from disk.
    ///
    /// This method loads an existing journal log from the specified directory, including all segments
    /// and checkpoint information.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition this journal log belongs to
    /// * `recover_point` - The recovery point offset to resume from
    /// * `split_offset` - The offset where log splitting should occur
    /// * `dir` - The directory path containing the log files
    /// * `index_file_max_size` - Maximum size in bytes for index files
    ///
    /// # Returns
    ///
    /// Returns the loaded journal log wrapped in `AppResult`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    /// use your_crate::{JournalLog, TopicPartition};
    ///
    /// let tp = TopicPartition::new("test", 0);
    /// let log = JournalLog::load_from(
    ///     &tp,
    ///     0,  // recover_point
    ///     0,  // split_offset
    ///     "/path/to/logs",
    ///     1024 * 1024  // 1MB index file size
    /// )?;
    /// ```
    pub fn load_from(
        topic_partition: &TopicPartition,
        recover_point: i64,
        split_offset: i64,
        dir: impl AsRef<Path>,
        index_file_max_size: u32,
    ) -> AppResult<Self> {
        // load segments
        let (segments, active_segment) =
            Self::load_segments(topic_partition, &dir, index_file_max_size as usize)?;

        // load checkpoint file
        let checkpoint_file = Self::load_checkpoint_file(topic_partition)?;
        let queue_next_offset = checkpoint_file.read_checkpoints(LogType::Journal)?;

        trace!(
            "load journal log queue_next_offset: {:?}",
            queue_next_offset
        );

        // determine log start offset
        let log_start_offset = segments
            .first_key_value()
            .map(|(offset, _)| *offset)
            .or_else(|| active_segment.as_ref().map(|s| s.base_offset()))
            .unwrap_or(0);

        // if no active segment, create new log
        if active_segment.is_none() {
            return JournalLog::new(topic_partition);
        }

        let metadata = JournalLogMetadata {
            queue_next_offset_info: DashMap::from_iter(queue_next_offset),
            queue_next_offset_checkpoints: checkpoint_file,
            log_start_offset,
            next_offset: recover_point + 1,
            recover_point,
            split_offset,
        };

        // build log
        let log = JournalLog::open(segments, active_segment.unwrap(), topic_partition, metadata)?;

        info!(
            "loaded journal log:{} next_offset:{}, recover_point:{}, split_offset:{}",
            topic_partition.id(),
            log.next_offset.load(),
            log.recover_point.load(),
            log.split_offset.load()
        );

        Ok(log)
    }

    /// Checkpoints the next offset information for queue topic partitions.
    ///
    /// This method writes the current next offset information for all queue topic partitions
    /// to the checkpoint file. This helps maintain durability and enables recovery after restarts.
    ///
    /// # Returns
    ///
    /// Returns `AppResult<()>` indicating success or failure of the checkpoint operation.
    ///
    /// # Errors
    ///
    /// Returns an error if writing the checkpoint fails, wrapped in `AppError::DetailedIoError`
    pub async fn checkpoint_next_offset(&self) -> AppResult<()> {
        let queue_next_offset_info = self
            .queue_next_offset_info
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();
        self.queue_next_offset_checkpoints
            .write_checkpoints(queue_next_offset_info)
            .await
            .map_err(|e| AppError::DetailedIoError(format!("write checkpoint error: {}", e)))
    }

    /// load checkpoint file
    fn load_checkpoint_file(topic_partition: &TopicPartition) -> AppResult<CheckPointFile> {
        let path = format!(
            "{}/{}",
            topic_partition.partition_dir(),
            NEXT_OFFSET_CHECKPOINT_FILE_NAME
        );
        Ok(CheckPointFile::new(path))
    }

    /// load segments
    fn load_segments(
        topic_partition: &TopicPartition,
        dir: impl AsRef<Path>,
        max_index_file_size: usize,
    ) -> AppResult<(
        BTreeMap<i64, ReadOnlySegmentIndex>,
        Option<ActiveSegmentIndex>,
    )> {
        let (index_files, log_files) = Self::scan_segment_files(dir)?;

        if log_files.is_empty() {
            return Ok((BTreeMap::new(), None));
        }

        Self::build_segments(topic_partition, index_files, log_files, max_index_file_size)
    }

    /// scan directory to get segment files
    fn scan_segment_files(dir: impl AsRef<Path>) -> AppResult<(BTreeSet<i64>, BTreeSet<i64>)> {
        let mut index_files = BTreeSet::new();
        let mut log_files = BTreeSet::new();

        let read_dir = std::fs::read_dir(&dir).map_err(|e| {
            AppError::DetailedIoError(format!(
                "read dir: {} error: {} while loading journal log",
                dir.as_ref().display(),
                e
            ))
        })?;

        for entry in read_dir.flatten() {
            if !entry.file_type()?.is_file() {
                continue;
            }

            let file_name = entry.file_name().to_string_lossy().to_string();
            if let Some((base_offset, file_type)) = Self::parse_segment_filename(&file_name) {
                match file_type {
                    SegmentFileType::Index => index_files.insert(base_offset),
                    SegmentFileType::Log => log_files.insert(base_offset),
                    SegmentFileType::TimeIndex => continue, // journal log don't use time index
                    SegmentFileType::Unknown => continue,
                };
            }
        }

        Ok((index_files, log_files))
    }

    /// parse segment filename
    fn parse_segment_filename(filename: &str) -> Option<(i64, SegmentFileType)> {
        let (prefix, suffix) = filename.rsplit_once('.')?;
        let base_offset = prefix.parse().ok()?;
        let file_type = match suffix {
            "index" => Some(SegmentFileType::Index),
            "log" => Some(SegmentFileType::Log),
            "timeindex" => Some(SegmentFileType::TimeIndex),
            _ => None,
        }?;
        Some((base_offset, file_type))
    }

    /// build segments
    fn build_segments(
        topic_partition: &TopicPartition,
        index_files: BTreeSet<i64>,
        log_files: BTreeSet<i64>,
        max_index_file_size: usize,
    ) -> AppResult<(
        BTreeMap<i64, ReadOnlySegmentIndex>,
        Option<ActiveSegmentIndex>,
    )> {
        let mut segments = BTreeMap::new();
        let mut active_segment = None;

        // only one log segment, as active segment
        if log_files.len() == 1 {
            let base_offset = *log_files.first().unwrap();
            if index_files.contains(&base_offset) {
                active_segment = Some(Self::create_active_segment(
                    topic_partition,
                    base_offset,
                    max_index_file_size,
                )?);
            }
            return Ok((segments, active_segment));
        }

        // multiple log segments, the latest as active segment
        for base_offset in log_files.iter().rev() {
            if !index_files.contains(base_offset) {
                continue;
            }

            if active_segment.is_none() {
                active_segment = Some(Self::create_active_segment(
                    topic_partition,
                    *base_offset,
                    max_index_file_size,
                )?);
            } else {
                segments.insert(
                    *base_offset,
                    Self::create_readonly_segment(topic_partition, *base_offset)?,
                );
            }
        }

        Ok((segments, active_segment))
    }

    /// create active segment
    fn create_active_segment(
        topic_partition: &TopicPartition,
        base_offset: i64,
        max_index_file_size: usize,
    ) -> AppResult<ActiveSegmentIndex> {
        let index_file_name = format!("{}/{}.index", topic_partition.partition_dir(), base_offset);
        let index_file = WritableIndexFile::new(index_file_name, max_index_file_size)?;
        ActiveSegmentIndex::open(base_offset, index_file, None)
    }

    /// create readonly segment
    fn create_readonly_segment(
        topic_partition: &TopicPartition,
        base_offset: i64,
    ) -> AppResult<ReadOnlySegmentIndex> {
        let index_file_name = format!("{}/{}.index", topic_partition.partition_dir(), base_offset);
        let index_file = ReadOnlyIndexFile::new(index_file_name)?;
        Ok(ReadOnlySegmentIndex::open(base_offset, index_file))
    }
}

#[cfg(test)]
mod tests {
    use crate::log::SegmentFileType;

    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_parse_segment_filename() {
        assert_eq!(
            JournalLog::parse_segment_filename("100.index"),
            Some((100, SegmentFileType::Index))
        );
        assert_eq!(
            JournalLog::parse_segment_filename("100.log"),
            Some((100, SegmentFileType::Log))
        );
        assert_eq!(JournalLog::parse_segment_filename("invalid"), None);
    }

    #[test]
    fn test_load_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let topic_partition = TopicPartition::new("test", 0, LogType::Journal);

        let result = JournalLog::load_segments(&topic_partition, temp_dir.path(), 1024);

        assert!(result.is_ok());
        let (segments, active_segment) = result.unwrap();
        assert!(segments.is_empty());
        assert!(active_segment.is_none());
    }
}
