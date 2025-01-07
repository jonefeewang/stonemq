use crate::log::index_file::{ReadOnlyIndexFile, WritableIndexFile};
use crate::message::TopicPartition;
use crate::{global_config, AppResult};
use crossbeam::atomic::AtomicCell;
use tracing::trace;

use super::{LogType, INDEX_FILE_SUFFIX};

/// define the common behavior of segment index
pub trait SegmentIndexCommon {
    fn base_offset(&self) -> i64;
    fn lookup_index(&self, relative_offset: u32) -> Option<(u32, u32)>;

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
pub struct ReadOnlySegmentIndex {
    base_offset: i64,
    offset_index: ReadOnlyIndexFile,
}

#[derive(Debug)]
pub struct ActiveSegmentIndex {
    base_offset: i64,
    offset_index: WritableIndexFile,
    bytes_since_last_index_entry: AtomicCell<usize>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PositionInfo {
    pub base_offset: i64,
    pub offset: i64,
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
    pub fn open(base_offset: i64, offset_index: ReadOnlyIndexFile) -> Self {
        Self {
            base_offset,
            offset_index,
        }
    }
}

impl ActiveSegmentIndex {
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
    /// open a new active log segment index
    pub fn open(
        base_offset: i64,
        offset_index: WritableIndexFile,
        _time_index: Option<WritableIndexFile>,
    ) -> AppResult<Self> {
        Ok(Self {
            base_offset,
            offset_index,
            bytes_since_last_index_entry: AtomicCell::new(0),
        })
    }

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

        if index_interval <= self.bytes_since_last_index_entry.load() {
            // if true {
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

            self.bytes_since_last_index_entry.store(0);
        }
        self.bytes_since_last_index_entry.fetch_add(records_size);

        Ok(())
    }

    pub fn offset_index_full(&self) -> bool {
        self.offset_index.is_full()
    }

    /// 将活动段转换为只读段
    pub fn into_readonly(self) -> AppResult<ReadOnlySegmentIndex> {
        let readonly_offset_index = self.offset_index.into_readonly()?;

        Ok(ReadOnlySegmentIndex {
            base_offset: self.base_offset,
            offset_index: readonly_offset_index,
        })
    }

    pub fn flush_index(&mut self) -> AppResult<()> {
        // flush offset index
        self.offset_index.flush()?;
        Ok(())
    }

    pub fn close(&mut self) -> AppResult<()> {
        self.offset_index.close()?;
        Ok(())
    }
}
