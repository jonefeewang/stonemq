use crate::log::file_records::FileRecords;
use crate::log::log_segment::PositionInfo;
use crate::log::{JournalLog, Log, QueueLog};
use crate::message::{MemoryRecords, TopicPartition};
use crate::{global_config, AppError, AppResult};
use bytes::{Buf, BytesMut};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::AsyncReadExt;
use tokio::time::{sleep, Duration};

/// Splitter的读取和消费的读取还不太一样
/// 1.Splitter的读取是一个读取者，而且连续的读取，所以针对一个journal log 最好每个splitter任务自己维护一个ReadBuffer
/// 而不需要通过FileRecord来读取，自己只要知道从哪个segment开始读取即可，然后读取到哪个位置，然后读取下一个segment
/// 2.消费的读取是多个并发读取者，而且不连续的数据.可以维护一个BufReader pool，然后读取者从pool中获取一个BufReader
/// .比如最热active segment, 可能有多个BufReader，而其他segment可能只有一个BufReader
pub struct SplitterTask {
    journal_log: Arc<JournalLog>,
    queue_logs: BTreeMap<TopicPartition, Arc<QueueLog>>,
    topic_partition: TopicPartition,
}
impl SplitterTask {
    pub fn new(
        journal_log: Arc<JournalLog>,
        queue_logs: BTreeMap<TopicPartition, Arc<QueueLog>>,
        topic_partition: TopicPartition,
    ) -> Self {
        SplitterTask {
            journal_log,
            queue_logs,
            topic_partition,
        }
    }

    pub async fn run(&mut self) -> AppResult<()> {
        let mut target_offset = self.journal_log.split_offset.load() + 1;

        loop {
            let position_info = self.journal_log.get_position_info(target_offset).await?;
            match self
                .read_and_process_segment(target_offset, &position_info)
                .await
            {
                Ok(last_offset) => {
                    target_offset = last_offset + 1;
                }
                Err(e) => {
                    if let Some(std_err) =
                        e.source().and_then(|e| e.downcast_ref::<std::io::Error>())
                    {
                        if std_err.kind() == ErrorKind::UnexpectedEof {
                            // 如果是EOF错误，则等待继续重试,
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    // 返回最后一个读到的offset
    async fn read_and_process_segment(
        &self,
        target_offset: u64,
        position_info: &PositionInfo,
    ) -> AppResult<u64> {
        let journal_topic_dir = PathBuf::from(global_config().log.journal_base_dir.clone())
            .join(self.topic_partition.id());
        let segment_path = journal_topic_dir.join(format!("{}.log", position_info.base_offset));
        let journal_seg_file = fs::File::open(segment_path).await?;

        let mut journal_seg_file =
            FileRecords::seek_journal(journal_seg_file, target_offset, position_info).await?;
        let mut last_read_offset = target_offset - 1;

        loop {
            match self.read_and_process_batch(&mut journal_seg_file).await {
                Ok(batch_end_offset) => {
                    last_read_offset = batch_end_offset;
                }
                Err(e) => {
                    if let Some(io_err) =
                        e.source().and_then(|e| e.downcast_ref::<std::io::Error>())
                    {
                        if io_err.kind() == ErrorKind::UnexpectedEof {
                            // 如果当前是active的segment的，就一直等待，直到active segment切换
                            if self.is_active_segment(position_info.base_offset).await? {
                                sleep(Duration::from_millis(100)).await;
                                continue;
                            } else {
                                // 如果当前segment不是active segment，则直接返回, 直到读取到最后一个offset，
                                // 这里有可能会导致切换到下一个segment，这也是整个读取过程中切换segment的唯一地方
                                return Ok(last_read_offset);
                            }
                        }
                    } else {
                        return Err(e.into());
                    }
                }
            }
        }
    }

    async fn read_and_process_batch(&self, file: &mut File) -> AppResult<u64> {
        let batch_size = file.read_u32().await?;
        let mut buf = BytesMut::with_capacity(batch_size as usize);
        // 这里有可能会报读到EOF错误，然后返回
        file.read_exact(&mut buf).await?;

        let (offset, tp_str) = Self::read_topic_partition(&mut buf).await;
        let topic_partition = TopicPartition::from_string(Cow::Owned(tp_str)).unwrap();

        self.write_queue_log(topic_partition, offset, buf).await?;
        self.journal_log.split_offset.store(offset + 1);

        Ok(offset)
    }

    async fn is_active_segment(&self, base_offset: u64) -> AppResult<bool> {
        let current_active_seg_offset = self.journal_log.current_active_seg_offset().await?;
        Ok(base_offset == current_active_seg_offset)
    }

    async fn write_queue_log(
        &self,
        topic_partition: TopicPartition,
        offset: u64,
        buffer: BytesMut,
    ) -> AppResult<()> {
        if let Some(queue_log) = self.queue_logs.get(&topic_partition) {
            let records = MemoryRecords::new(buffer);
            queue_log
                .append_records((topic_partition, offset, records))
                .await?;
        } else {
            return Err(AppError::IllegalStateError(
                format!(
                    "Queue log not found for topic partition: {}",
                    topic_partition
                )
                .into(),
            ));
        }

        Ok(())
    }

    async fn read_topic_partition(buf: &mut BytesMut) -> (u64, String) {
        let offset = buf.get_u64();
        let tp_str_size = buf.get_u32();
        let mut tp_str_bytes = vec![0; tp_str_size as usize];
        buf.copy_to_slice(&mut tp_str_bytes);
        (offset, String::from_utf8(tp_str_bytes).unwrap())
    }
}
