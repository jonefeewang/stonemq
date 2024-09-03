use crate::log::file_records::FileRecords;
use crate::log::{FileOp};
use crate::message::MemoryRecords;
use crate::message::TopicPartition;
use crate::{global_config, AppResult};
use tokio::sync::oneshot;
use tracing::trace;
use crate::AppError::IllegalStateError;
use crate::log::index_file::IndexFile;

#[derive(Debug)]
pub struct LogSegment {
    log_dir: String,
    file_records: FileRecords,
    base_offset: u64,
    time_index: Option<IndexFile>,
    offset_index: Option<IndexFile>,
    bytes_since_last_index_entry: usize,
}

impl LogSegment {
    pub fn size(&self) -> usize {
        self.file_records.size()
    }
    pub async fn new_journal_seg(dir: String, base_offset: u64, create: bool) -> AppResult<Self> {
        let file_name = format!("{}/{}.log", dir, base_offset);
        let index_file_name = format!("{}/{}.index", dir, base_offset);
        let index_file_size=global_config().log.journal_index_file_size;
        trace!("new segment file:{}", file_name);
        if create {
            trace!(
                "file dose not exist, create one journal segment file:{}",
                file_name
            );
        }
        let file_records = FileRecords::open(file_name).await?;
        let segment = LogSegment {
            log_dir: dir.to_string(),
            file_records,
            base_offset,
            time_index: None,
            offset_index: Some(IndexFile::new(index_file_name, index_file_size).await?),
            bytes_since_last_index_entry: 0,
        };
        Ok(segment)
    }
    pub async fn new_queue_seg(dir: String, base_offset: u64, create: bool) -> AppResult<Self> {
        let file_name = format!("{}/{}.log", dir, base_offset);
        if create {
            trace!("create queue segment file:{}", file_name);
        }
        let file_records = FileRecords::open(file_name).await?;
        let segment = LogSegment {
            log_dir: dir,
            file_records,
            base_offset,
            time_index: None,
            offset_index: None,
            bytes_since_last_index_entry: 0,
        };
        Ok(segment)
    }
    pub async fn append_record(
        &self,
        records_package: (
            TopicPartition,
            MemoryRecords,
            oneshot::Sender<AppResult<()>>,
        ),
    ) -> AppResult<()> {
        let records_size = records_package.1.size();
        self.bytes_since_last_index_entry += records_size;



        self.file_records
            .tx
            .send(FileOp::AppendRecords(records_package))
            .await?;
        Ok(())
    }
    pub(crate) async fn close_write(&self) {
        self.file_records.close_write().await;
    }
    pub(crate) async fn flush(&self) -> AppResult<(u64)> {
        let (tx, rx) = oneshot::channel::<AppResult<(u64)>>();
        self.file_records.tx.send(FileOp::Flush(tx)).await?;
        let size = rx.await??;
        Ok(size)
    }
}
