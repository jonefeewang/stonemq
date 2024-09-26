use crate::log::file_records::FileRecords;
use crate::log::log_segment::PositionInfo;
use crate::log::{JournalLog, Log, QueueLog};
use crate::message::{MemoryRecords, TopicPartition};
use crate::{global_config, AppError, AppResult, Shutdown};
use bytes::{Buf, BytesMut};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::time::{sleep, Duration, Interval};
use tracing::{debug, span, trace, Level};

/// Splitter的读取和消费的读取还不太一样
/// 1.Splitter的读取是一个读取者，而且连续的读取，所以针对一个journal log 最好每个splitter任务自己维护一个ReadBuffer
/// 而不需要通过FileRecord来读取，自己只要知道从哪个segment开始读取即可，然后读取到哪个位置，然后读取下一个segment
/// 2.消费的读取是多个并发读取者，而且不连续的数据.可以维护一个BufReader pool，然后读取者从pool中获取一个BufReader
/// .比如最热active segment, 可能有多个BufReader，而其他segment可能只有一个BufReader
pub struct SplitterTask {
    journal_log: Arc<JournalLog>,
    queue_logs: BTreeMap<TopicPartition, Arc<QueueLog>>,
    topic_partition: TopicPartition,
    read_wait_interval: Interval,
}
impl SplitterTask {
    pub fn new(
        journal_log: Arc<JournalLog>,
        queue_logs: BTreeMap<TopicPartition, Arc<QueueLog>>,
        topic_partition: TopicPartition,
        read_wait_interval: Interval,
    ) -> Self {
        SplitterTask {
            journal_log,
            queue_logs,
            topic_partition,
            read_wait_interval,
        }
    }

    #[tracing::instrument(level = "trace", skip(self, shutdown), fields(topic_partition = self.topic_partition.to_string()))]
    pub async fn run(&mut self, mut shutdown: Shutdown) -> AppResult<()> {
        // sleep(Duration::from_millis(1000 * 1000)).await;

        loop {
            trace!(
                "splitter 进入循环，split offset: {}",
                self.journal_log.split_offset.load()
            );
            // split offset 初始值为-1， 表示还没有初始化
            let target_offset = self.journal_log.split_offset.load() + 1;
            if shutdown.is_shutdown() {
                return Ok(());
            }

            let position_info = self.journal_log.get_position_info(target_offset).await;
            let position_info = match position_info {
                Ok(position_info) => position_info,
                Err(e) => {
                    if let Some(std_err) =
                        e.source().and_then(|e| e.downcast_ref::<std::io::Error>())
                    {
                        if std_err.kind() == ErrorKind::NotFound {
                            trace!(
                                "未能找到offset:{} 的position信息， 等待继续重试",
                                target_offset
                            );
                            if shutdown.is_shutdown() {
                                return Ok(());
                            }
                            self.read_wait_interval.tick().await;

                            continue;
                        }
                    }
                    return Err(e);
                }
            };

            match self
                .read_and_process_segment(target_offset, &position_info, &mut shutdown)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    if let Some(std_err) =
                        e.source().and_then(|e| e.downcast_ref::<std::io::Error>())
                    {
                        if std_err.kind() == ErrorKind::UnexpectedEof
                        // 尝试读取下一个offset，但是找不到，因为已到达active segment的最后一个offset
                            || std_err.kind() == ErrorKind::NotFound
                        {
                            if shutdown.is_shutdown() {
                                return Ok(());
                            }
                            trace!("读取segment时遇到EOF错误， 等待继续重试");
                            tokio::select! {
                                _ = self.read_wait_interval.tick() => {},
                                _ = shutdown.recv() => {
                                    return Ok(());
                                }
                            };

                            // 如果是EOF错误，则等待继续重试,
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn read_and_process_segment(
        &mut self,
        target_offset: i64,
        position_info: &PositionInfo,
        shutdown: &mut Shutdown,
    ) -> AppResult<()> {
        let journal_topic_dir = PathBuf::from(global_config().log.journal_base_dir.clone())
            .join(self.topic_partition.id());
        let segment_path = journal_topic_dir.join(format!("{}.log", position_info.base_offset));
        let journal_seg_file = fs::File::open(&segment_path).await?;

        debug!(
            "开始读取一个segment{:?} ref:  {:?}, target offset: {}",
            segment_path, &position_info, target_offset
        );

        // 这里会报UnexpectedEof错误，然后返回，也会报NotFound错误
        let mut journal_seg_file =
            FileRecords::seek_journal(journal_seg_file, target_offset, position_info).await?;

        trace!(
            "文件内部读指针位置: {}",
            journal_seg_file.stream_position().await?
        );

        loop {
            if shutdown.is_shutdown() {
                return Ok(());
            }
            let ret = tokio::select! {
                read_result = self.read_batch(&mut journal_seg_file) => read_result?,
                _ = shutdown.recv() => {
                    return Ok(());
                }
            };

            let (journal_offset, topic_partition, buf) = ret;

            let ret = self
                .process_batch(journal_offset, topic_partition, buf)
                .await;

            match ret {
                Ok(_) => {}
                Err(e) => {
                    if let Some(io_err) =
                        e.source().and_then(|e| e.downcast_ref::<std::io::Error>())
                    {
                        if io_err.kind() == ErrorKind::UnexpectedEof {
                            // 如果当前是active的segment的，就一直等待，直到active segment切换
                            if self.is_active_segment(position_info.base_offset).await? {
                                if shutdown.is_shutdown() {
                                    return Ok(());
                                }
                                trace!("当前segment是active segment，等待继续重试");
                                self.read_wait_interval.tick().await;
                                continue;
                            } else {
                                // 如果当前segment不是active segment，则直接返回, 直到读取到最后一个offset，
                                // 这里有可能会导致切换到下一个segment，这也是整个读取过程中切换segment的唯一地方
                                // 这里需要返回，否则会一直循环
                                return Ok(());
                            }
                        }
                    } else {
                        return Err(e);
                    }
                }
            }
        }
    }

    async fn read_batch(&self, file: &mut File) -> AppResult<(i64, TopicPartition, BytesMut)> {
        let batch_size = file.read_u32().await?;
        let mut buf = BytesMut::zeroed(batch_size as usize);
        // 这里有可能会报读到EOF错误，然后返回
        file.read_exact(&mut buf).await?;

        let (journal_offset, tp_str) = Self::read_topic_partition(&mut buf).await;
        let topic_partition = TopicPartition::from_string(Cow::Owned(tp_str)).unwrap();

        Ok((journal_offset, topic_partition, buf))
    }

    async fn process_batch(
        &self,
        journal_offset: i64,
        topic_partition: TopicPartition,
        buf: BytesMut,
    ) -> AppResult<()> {
        let record_count = self.write_queue_log(topic_partition, buf).await?;

        self.journal_log
            .split_offset
            .store(journal_offset + record_count as i64 - 1);
        trace!(
            "写入queue log成功， {},更新split offset: {}",
            record_count,
            self.journal_log.split_offset.load()
        );
        Ok(())
    }

    async fn is_active_segment(&self, base_offset: i64) -> AppResult<bool> {
        let current_active_seg_offset = self.journal_log.current_active_seg_offset().await?;
        Ok(base_offset == current_active_seg_offset)
    }

    async fn write_queue_log(
        &self,
        topic_partition: TopicPartition,
        buffer: BytesMut,
    ) -> AppResult<i32> {
        return if let Some(queue_log) = self.queue_logs.get(&topic_partition) {
            let records = MemoryRecords::new(buffer);
            let record_count = records.records_count();
            let offset = records.base_offset();
            queue_log
                .append_records((topic_partition, offset, records))
                .await?;
            Ok(record_count)
        } else {
            Err(AppError::IllegalStateError(
                format!(
                    "Queue log not found for topic partition: {}",
                    topic_partition
                )
                .into(),
            ))
        };
    }

    async fn read_topic_partition(buf: &mut BytesMut) -> (i64, String) {
        let journal_offset = buf.get_i64();
        let tp_str_size = buf.get_u32();
        let mut tp_str_bytes = vec![0; tp_str_size as usize];
        buf.copy_to_slice(&mut tp_str_bytes);
        (journal_offset, String::from_utf8(tp_str_bytes).unwrap())
    }
}
