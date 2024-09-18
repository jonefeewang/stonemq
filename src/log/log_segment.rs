use crate::log::file_records::FileRecords;
use crate::log::index_file::IndexFile;
use crate::log::FileOp;
use crate::message::MemoryRecords;
use crate::message::TopicPartition;
use crate::AppError::CommonError;
use crate::{global_config, AppResult};
use crossbeam_utils::atomic::AtomicCell;
use std::path::PathBuf;
use tokio::sync::oneshot;
use tracing::trace;

#[derive(Debug)]
pub struct LogSegment {
    topic_partition: TopicPartition,
    file_records: FileRecords,
    base_offset: u64,
    time_index: Option<IndexFile>,
    offset_index: IndexFile,
    bytes_since_last_index_entry: AtomicCell<usize>,
}

pub struct PositionInfo {
    pub base_offset: u64,
    pub offset: u64,
    pub position: u32,
}

impl LogSegment {
    pub async fn get_position(&self, offset: u64) -> AppResult<PositionInfo> {
        let offset_position = self
            .offset_index
            .lookup((offset - self.base_offset) as u32)
            .await
            .ok_or(CommonError(format!(
                "can not find offset:{} in index file: {}",
                offset, self.base_offset
            )))?;
        let pos_info = PositionInfo {
            base_offset: self.base_offset,
            offset: offset_position.0 as u64 + self.base_offset,
            position: offset_position.1,
        };
        Ok(pos_info)
    }
    pub fn size(&self) -> usize {
        self.file_records.size()
    }
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub(crate) async fn offset_index_full(&self) -> AppResult<bool> {
        self.offset_index.is_full().await
    }

    pub async fn new_journal_seg(
        topic_partition: &TopicPartition,
        base_offset: u64,
        create: bool,
    ) -> AppResult<Self> {
        let config = global_config();
        let partition_dir =
            PathBuf::from(&config.log.journal_base_dir).join(topic_partition.to_string());
        let file_name = partition_dir.join(format!("{}.log", base_offset));
        let index_file_name = partition_dir.join(format!("{}.index", base_offset));
        let index_file_max_size = config.log.journal_index_file_size;

        if create {
            trace!("Creating new journal segment file: {}", file_name.display());
        } else {
            trace!(
                "Opening existing journal segment file: {}",
                file_name.display()
            );
        }

        let file_records = FileRecords::open(&file_name).await?;
        let offset_index = IndexFile::new(&index_file_name, index_file_max_size, false).await?;

        Ok(Self {
            topic_partition: topic_partition.clone(),
            file_records,
            base_offset,
            time_index: None,
            offset_index,
            bytes_since_last_index_entry: AtomicCell::new(0),
        })
    }
    pub async fn new_queue_seg(
        topic_partition: &TopicPartition,
        base_offset: u64,
        create: bool,
    ) -> AppResult<Self> {
        let config = &global_config().log;
        let partition_dir = PathBuf::from(&config.queue_base_dir).join(topic_partition.to_string());
        let file_name = partition_dir.join(format!("{}.log", base_offset));
        let index_file_name = partition_dir.join(format!("{}.index", base_offset));
        let index_file_max_size = config.journal_index_file_size;
        if create {
            trace!("create queue segment file:{}", file_name.display());
        }
        let file_records = FileRecords::open(file_name).await?;
        let segment = LogSegment {
            topic_partition: topic_partition.clone(),
            file_records,
            base_offset,
            time_index: None,
            offset_index: IndexFile::new(index_file_name, index_file_max_size, false).await?,
            bytes_since_last_index_entry: AtomicCell::new(0),
        };
        Ok(segment)
    }
    pub async fn append_record(
        &self,
        records_package: (
            u64,
            TopicPartition,
            MemoryRecords,
            oneshot::Sender<AppResult<()>>,
        ),
    ) -> AppResult<()> {
        // 计算是否更新index file
        let memory_records = &records_package.2;
        let records_size = memory_records.size();
        self.bytes_since_last_index_entry.fetch_add(records_size);
        if self.bytes_since_last_index_entry.load()
            >= global_config().log.journal_index_interval_bytes
        {
            self.offset_index
                .add_entry(
                    memory_records.base_offset() as u32,
                    self.file_records.size() as u32,
                )
                .await?;
            if self.time_index.is_some() {
                self.time_index
                    .as_ref()
                    .unwrap()
                    .add_entry(
                        (records_package.0 - self.base_offset) as u32,
                        self.file_records.size() as u32,
                    )
                    .await?;
            }

            self.bytes_since_last_index_entry.store(0);
        }
        // 写入消息
        self.file_records
            .tx
            .send(FileOp::AppendRecords(records_package))
            .await?;
        Ok(())
    }

    pub(crate) async fn flush(&self) -> AppResult<u64> {
        let (tx, rx) = oneshot::channel::<AppResult<u64>>();
        self.file_records.tx.send(FileOp::Flush(tx)).await?;
        let size = rx.await??;
        self.offset_index.trim_to_valid_size().await?;
        Ok(size)
    }
}
