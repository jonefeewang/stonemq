use crate::log::file_records::FileRecords;
use crate::log::{FileOp, OffsetIndex, TimeIndex};
use crate::message::MemoryRecords;
use crate::message::TopicPartition;
use crate::AppResult;
use tokio::sync::oneshot;
use tracing::trace;

#[derive(Debug)]
pub struct LogSegment {
    log_dir: String,
    file_records: FileRecords,
    base_offset: u64,
    time_index: Option<TimeIndex>,
    offset_index: Option<OffsetIndex>,
    index_interval_bytes: i32,
}

impl LogSegment {
    pub fn size(&self) -> usize {
        self.file_records.size()
    }
    pub async fn new_journal_seg(dir: String, base_offset: u64, create: bool) -> AppResult<Self> {
        let file_name = format!("{}/{}.log", dir, base_offset);
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
            offset_index: None,
            index_interval_bytes: 0,
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
            index_interval_bytes: 0,
        };
        Ok(segment)
    }
    pub async fn append_record(
        &self,
        records: (
            TopicPartition,
            MemoryRecords,
            oneshot::Sender<AppResult<()>>,
        ),
    ) -> AppResult<()> {
        self.file_records
            .tx
            .send(FileOp::AppendRecords(records))
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
