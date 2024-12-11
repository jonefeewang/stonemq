use crate::log::file_records::FileRecords;
use crate::log::index_file::IndexFile;
use crate::message::MemoryRecords;
use crate::message::TopicPartition;
use crate::AppError;
use crate::{global_config, AppResult};
use crossbeam::atomic::AtomicCell;
use std::path::Path;
use std::path::PathBuf;
use tracing::trace;

use super::LogType;

#[derive(Debug)]
pub struct LogSegment {
    _topic_partition: TopicPartition,
    file_records: Option<FileRecords>,
    base_offset: i64,
    time_index: Option<IndexFile>,
    offset_index: IndexFile,
    bytes_since_last_index_entry: AtomicCell<usize>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PositionInfo {
    pub base_offset: i64,
    pub offset: i64,
    pub position: i64,
}

impl LogSegment {
    pub fn open(
        topic_partition: TopicPartition,
        base_offset: i64,
        offset_index: IndexFile,
        time_index: Option<IndexFile>,
    ) -> Self {
        Self {
            _topic_partition: topic_partition,
            base_offset,
            file_records: None,
            offset_index,
            time_index,
            bytes_since_last_index_entry: AtomicCell::new(0),
        }
    }
    // 如果未找到的话，offset_index内容为空
    /// Retrieves the position information for a given offset.
    ///
    /// This function looks up the position in the offset index file for the specified offset.
    /// If the offset is not found, it returns an error.
    ///
    /// # Arguments
    ///
    /// * `offset` - The offset to look up.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the `PositionInfo` if successful, or an error if the offset is not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the offset cannot be found in the index file.In this case, the content of offset_index should be empty.
    ///
    ///
    pub async fn get_relative_position(&self, offset: i64) -> AppResult<PositionInfo> {
        let offset_position = self
            .offset_index
            .lookup((offset - self.base_offset) as u32)
            .await
            .ok_or(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "can not find offset:{} in index file: {}",
                    offset, self.base_offset
                ),
            ))?;
        let pos_info = PositionInfo {
            base_offset: self.base_offset,
            offset: offset_position.0 as i64 + self.base_offset,
            position: offset_position.1 as i64,
        };
        Ok(pos_info)
    }
    pub fn size(&self) -> AppResult<usize> {
        if self.file_records.is_none() {
            return Err(AppError::InvalidOperation(format!(
                "inactive segment can not get size:{}",
                self.base_offset
            )));
        }
        Ok(self.file_records.as_ref().unwrap().size())
    }
    pub fn base_offset(&self) -> i64 {
        self.base_offset
    }

    pub(crate) async fn offset_index_full(&self) -> AppResult<bool> {
        self.offset_index.is_full().await
    }

    /// 运行时创建一个新的segment
    pub async fn new(
        topic_partition: &TopicPartition,
        dir: impl AsRef<Path>,
        base_offset: i64,
        index_file_max_size: u32,
    ) -> AppResult<Self> {
        let dir = PathBuf::from(dir.as_ref());
        let file_name = dir.join(format!("{}.log", base_offset));
        let index_file_name = dir.join(format!("{}.index", base_offset));
        let file_records = FileRecords::open(file_name).await?;
        let offset_index = IndexFile::new(index_file_name, index_file_max_size as usize, false)
            .await
            .map_err(|e| AppError::DetailedIoError(format!("open index file error: {}", e)))?;
        let segment = LogSegment {
            _topic_partition: topic_partition.clone(),
            file_records: Some(file_records),
            base_offset,
            time_index: None,
            offset_index,
            bytes_since_last_index_entry: AtomicCell::new(0),
        };
        Ok(segment)
    }
    pub async fn append_record(
        &mut self,
        log_type: LogType,
        records_package: (
            i64, //
            TopicPartition,
            i64, // first batch queue base offset
            i64, // last batch queue base offset
            u32, // records count
            MemoryRecords,
        ),
    ) -> AppResult<()> {
        if self.file_records.is_none() {
            return Err(AppError::InvalidOperation(format!(
                "inactive segment can not append record:{}",
                self.base_offset
            )));
        }

        // 计算是否更新index file

        let records_size = records_package.5.size();
        let first_offset = match log_type {
            LogType::Journal => records_package.0, // journal offset
            LogType::Queue => records_package.2,   // first batch queue base offset
        };
        let relative_offset = first_offset - self.base_offset;

        #[allow(unused_variables)]
        let index_interval = match log_type {
            LogType::Journal => global_config().log.journal_index_interval_bytes,
            LogType::Queue => global_config().log.queue_index_interval_bytes,
        };

        if index_interval <= self.bytes_since_last_index_entry.load() {
            // if true {
            //正常情况下是不会满的，因为在写入之前会判断是否满了
            self.offset_index
                .add_entry(
                    relative_offset as u32,
                    (self.file_records.as_ref().unwrap().size()) as u32,
                )
                .await?;
            trace!(
                "write index entry: {},{},{:?},{},{}",
                relative_offset,
                self.file_records.as_ref().unwrap().size(),
                self.offset_index,
                index_interval,
                self.bytes_since_last_index_entry.load()
            );
            if self.time_index.is_some() {
                // TODO 时间索引
                self.time_index
                    .as_ref()
                    .unwrap()
                    .add_entry(
                        (first_offset - self.base_offset) as u32,
                        self.file_records.as_ref().unwrap().size() as u32,
                    )
                    .await?;
            }

            self.bytes_since_last_index_entry.store(0);
        }
        self.bytes_since_last_index_entry.fetch_add(records_size);

        // 写入消息
        match log_type {
            LogType::Journal => {
                self.file_records
                    .as_mut()
                    .unwrap()
                    .append_journal(
                        records_package.0,
                        records_package.1,
                        records_package.2,
                        records_package.3,
                        records_package.4,
                        records_package.5,
                    )
                    .await?;
            }
            LogType::Queue => {
                self.file_records
                    .as_mut()
                    .unwrap()
                    .append_queue(
                        records_package.0,
                        records_package.1,
                        records_package.2,
                        records_package.3,
                        records_package.4,
                        records_package.5,
                    )
                    .await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn flush(&self) -> AppResult<u64> {
        if self.file_records.is_none() {
            return Err(AppError::InvalidOperation(format!(
                "inactive segment can not flush:{}",
                self.base_offset
            )));
        }
        let size = self.file_records.as_ref().unwrap().flush().await?;
        self.offset_index.trim_to_valid_size().await?;
        self.offset_index.flush().await?;
        Ok(size)
    }
    pub async fn become_active(
        &mut self,
        fr_file_name: impl AsRef<Path>,
        index_file_name: impl AsRef<Path>,
        index_file_max_size: u32,
    ) -> AppResult<()> {
        let file_name = PathBuf::from(fr_file_name.as_ref());
        self.file_records = Some(FileRecords::open(file_name).await?);

        let new_index_file = IndexFile::new(index_file_name, index_file_max_size as usize, false)
            .await
            .map_err(|e| AppError::DetailedIoError(format!("open index file error: {}", e)))?;

        self.offset_index = new_index_file;
        Ok(())
    }
}
