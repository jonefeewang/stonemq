use crate::log::index_file::{ReadOnlyIndexFile, WritableIndexFile};
use crate::message::TopicPartition;
use crate::{global_config, AppResult};
use crossbeam::atomic::AtomicCell;
use tracing::trace;

use super::FILE_WRITER;
use super::{LogType, INDEX_FILE_SUFFIX};

/// 定义日志段的公共行为
pub trait LogSegmentCommon {
    fn base_offset(&self) -> i64;
    fn lookup_index(&self, relative_offset: u32) -> Option<(u32, u32)>;
    fn size(&self) -> u64;

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

#[derive(Debug)]
pub struct ReadOnlyLogSegment {
    topic_partition: TopicPartition,
    base_offset: i64,
    time_index: Option<ReadOnlyIndexFile>,
    offset_index: ReadOnlyIndexFile,
}

#[derive(Debug)]
pub struct ActiveLogSegment {
    topic_partition: TopicPartition,
    base_offset: i64,
    time_index: Option<WritableIndexFile>,
    offset_index: WritableIndexFile,
    bytes_since_last_index_entry: AtomicCell<usize>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PositionInfo {
    pub base_offset: i64,
    pub offset: i64,
    pub position: i64,
}

impl LogSegmentCommon for ReadOnlyLogSegment {
    fn base_offset(&self) -> i64 {
        self.base_offset
    }

    fn lookup_index(&self, relative_offset: u32) -> Option<(u32, u32)> {
        self.offset_index.lookup(relative_offset)
    }

    fn size(&self) -> u64 {
        FILE_WRITER.get_log_size(&self.topic_partition)
    }
}

impl LogSegmentCommon for ActiveLogSegment {
    fn base_offset(&self) -> i64 {
        self.base_offset
    }

    fn lookup_index(&self, relative_offset: u32) -> Option<(u32, u32)> {
        self.offset_index.lookup(relative_offset)
    }

    fn size(&self) -> u64 {
        FILE_WRITER.get_log_size(&self.topic_partition)
    }
}

impl ReadOnlyLogSegment {
    pub fn open(
        topic_partition: &TopicPartition,
        base_offset: i64,
        offset_index: ReadOnlyIndexFile,
        time_index: Option<ReadOnlyIndexFile>,
    ) -> Self {
        Self {
            topic_partition: topic_partition.clone(),
            base_offset,
            offset_index,
            time_index,
        }
    }

    fn size(&self) -> u64 {
        FILE_WRITER.get_log_size(&self.topic_partition)
    }
}

impl ActiveLogSegment {
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
        Self::open(topic_partition, base_offset, offset_index, None)
    }
    /// open a new active log segment
    pub fn open(
        topic_partition: &TopicPartition,
        base_offset: i64,
        offset_index: WritableIndexFile,
        _time_index: Option<WritableIndexFile>,
    ) -> AppResult<Self> {
        // open log file
        FILE_WRITER.open_file(&topic_partition, base_offset)?;

        Ok(Self {
            topic_partition: topic_partition.clone(),
            base_offset,
            time_index: None,
            offset_index,
            bytes_since_last_index_entry: AtomicCell::new(0),
        })
    }

    pub fn update_metadata(
        &self,
        records_size: usize,
        first_offset: i64,
        log_type: LogType,
    ) -> AppResult<()> {
        let segment_size = FILE_WRITER.get_log_size(&self.topic_partition);
        let relative_offset = first_offset - self.base_offset;

        let index_interval = match log_type {
            LogType::Journal => global_config().log.journal_index_interval_bytes,
            LogType::Queue => global_config().log.queue_index_interval_bytes,
        };

        if index_interval <= self.bytes_since_last_index_entry.load() {
            self.offset_index
                .add_entry(relative_offset as u32, segment_size as u32)?;

            trace!(
                "write index entry: {},{},{:?},{},{}",
                relative_offset,
                segment_size,
                self.offset_index,
                index_interval,
                self.bytes_since_last_index_entry.load()
            );

            if let Some(time_index) = &self.time_index {
                time_index.add_entry(
                    (first_offset - self.base_offset) as u32,
                    segment_size as u32,
                )?;
            }

            self.bytes_since_last_index_entry.store(0);
        }
        self.bytes_since_last_index_entry.fetch_add(records_size);

        Ok(())
    }

    pub fn offset_index_full(&self) -> bool {
        self.offset_index.is_full()
    }

    /// 将活动段转换为只读段
    pub fn into_readonly(self) -> AppResult<ReadOnlyLogSegment> {
        let readonly_offset_index = self.offset_index.into_readonly()?;
        let readonly_time_index = match self.time_index {
            Some(time_index) => Some(time_index.into_readonly()?),
            None => None,
        };

        Ok(ReadOnlyLogSegment {
            topic_partition: self.topic_partition,
            base_offset: self.base_offset,
            offset_index: readonly_offset_index,
            time_index: readonly_time_index,
        })
    }

    pub fn flush_index(&mut self) -> AppResult<()> {
        self.offset_index.flush()?;
        if let Some(time_index) = &self.time_index {
            time_index.flush()?;
        }
        Ok(())
    }
}
