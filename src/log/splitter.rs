use crate::log::log_segment::PositionInfo;
use crate::log::{JournalLog, LogType};
use crate::message::{MemoryRecords, TopicPartition};
use crate::{global_config, AppError, AppResult, Shutdown};
use bytes::{Buf, BytesMut};

use std::collections::BTreeMap;
use std::error::Error;
use std::io::{self, ErrorKind, Read};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::time::Interval;
use tracing::{debug, error, instrument, trace};

use super::queue_log::QueueLog;

#[derive(Debug)]
pub struct SplitterTask {
    journal_log: Arc<JournalLog>,
    queue_logs: BTreeMap<TopicPartition, Arc<QueueLog>>,
    topic_partition: TopicPartition,
    read_wait_interval: Interval,
    _shutdown_complete_tx: Sender<()>,
}
impl SplitterTask {
    pub fn new(
        journal_log: Arc<JournalLog>,
        queue_logs: BTreeMap<TopicPartition, Arc<QueueLog>>,
        topic_partition: TopicPartition,
        read_wait_interval: Interval,
        _shutdown_complete_tx: Sender<()>,
    ) -> Self {
        SplitterTask {
            journal_log,
            queue_logs,
            topic_partition,
            read_wait_interval,
            _shutdown_complete_tx,
        }
    }
    #[instrument(name = "splitter_run", skip_all, fields(target_offset))]
    pub async fn run(&mut self, mut shutdown: Shutdown) -> AppResult<()> {
        debug!(
            "splitter task for journal: {} and queues: {:?}",
            self.topic_partition.id(),
            self.queue_logs
                .keys()
                .map(|tp| tp.id())
                .collect::<Vec<String>>()
        );
        while !shutdown.is_shutdown() {
            let target_offset = self.journal_log.split_offset.load() + 1;
            debug!("start loop and read target offset: {}", target_offset);

            match self
                .process_target_offset(target_offset, &mut shutdown)
                .await
            {
                Ok(()) => continue,
                Err(e) if self.is_retrievable_error(&e) => {
                    trace!(
                        "读取目标offset失败，可恢复中: {}，interval: {:?}",
                        e,
                        self.read_wait_interval
                    );
                    if shutdown.is_shutdown() {
                        return Ok(());
                    }
                    tokio::select! {
                        _ = self.read_wait_interval.tick() => {
                            trace!("等待继续读取......");
                        },
                        _ = shutdown.recv() => {
                            println!("shutdown 收到信号");
                            return Ok(());
                        }
                    }
                    continue;
                }
                Err(e) => {
                    error!("读取目标offset失败，不no no可恢复: {}", e);
                    return Err(e);
                }
            }
        }
        debug!("splitter task exit");
        Ok(())
    }

    async fn process_target_offset(
        &mut self,
        target_offset: i64,
        shutdown: &mut Shutdown,
    ) -> AppResult<()> {
        let position_info = self.journal_log.get_relative_position_info(target_offset)?;

        self.read_and_process_segment(target_offset, position_info, shutdown)
            .await
    }

    fn is_retrievable_error(&self, e: &AppError) -> bool {
        trace!("is_retrievable_error: {:?}", e);
        e.source()
            .and_then(|e| e.downcast_ref::<std::io::Error>())
            .map_or(false, |std_err| {
                matches!(
                    std_err.kind(),
                    ErrorKind::NotFound | ErrorKind::UnexpectedEof
                )
            })
    }

    async fn read_and_process_segment(
        &mut self,
        target_offset: i64,
        position_info: PositionInfo,
        shutdown: &mut Shutdown,
    ) -> AppResult<()> {
        let journal_topic_dir = PathBuf::from(global_config().log.journal_base_dir.clone())
            .join(self.topic_partition.id());
        let segment_path = journal_topic_dir.join(format!("{}.log", position_info.base_offset));
        let journal_seg_file = std::fs::File::open(&segment_path)?;

        debug!(
            "开始读取一个segment{:?} ref:  {:?}, target offset: {}",
            segment_path, &position_info, target_offset
        );

        // 这里会报UnexpectedEof错误，然后返回，也会报NotFound错误
        let (_, mut current_position) = crate::log::seek(
            journal_seg_file,
            target_offset,
            position_info,
            LogType::Journal,
        )
        .await?;

        trace!("文件内部读指针位置: {}", current_position.position);

        loop {
            if shutdown.is_shutdown() {
                return Ok(());
            }
            let ret = tokio::select! {
                read_result = self.read_batch(&mut current_position) => read_result,
                _ = shutdown.recv() => {
                    return Ok(());
                }
            };

            match ret {
                Ok((
                    journal_offset,
                    topic_partition,
                    first_batch_queue_base_offset,
                    last_batch_queue_base_offset,
                    records_count,
                    recrods,
                )) => {
                    // 写入queue log失败怎么办？
                    self.process_batch(
                        journal_offset,
                        topic_partition,
                        first_batch_queue_base_offset,
                        last_batch_queue_base_offset,
                        records_count,
                        recrods,
                    )
                    .await?;
                }
                Err(e) => {
                    if let Some(io_err) =
                        e.source().and_then(|e| e.downcast_ref::<std::io::Error>())
                    {
                        trace!("io_err: {:?}", io_err);
                        if io_err.kind() == ErrorKind::UnexpectedEof {
                            // 如果当前是active的segment的，就一直等待，直到active segment切换
                            if self.is_active_segment(position_info.base_offset).await? {
                                if shutdown.is_shutdown() {
                                    return Ok(());
                                }
                                trace!("当前segment是active segment,等待继续重试");
                                tokio::select! {
                                    _ = self.read_wait_interval.tick() => {},
                                    _ = shutdown.recv() => return Ok(()),
                                }
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

    async fn read_batch(
        &self,
        position_info: &PositionInfo,
    ) -> AppResult<(i64, TopicPartition, i64, i64, u32, MemoryRecords)> {
        let file_name = format!(
            "{}/{}.log",
            self.topic_partition.partition_dir(),
            position_info.base_offset
        );
        let mut buf = tokio::task::spawn_blocking({
            move || -> io::Result<BytesMut> {
                let mut file = std::fs::File::open(file_name)?;
                let mut buffer = [0u8; 4];
                file.read_exact(&mut buffer)?;
                let batch_size = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
                let mut buf = BytesMut::zeroed(batch_size as usize);
                // 这里有可能会报读到EOF错误，然后返回
                file.read_exact(&mut buf)?;
                Ok(buf)
            }
        })
        .await
        .map_err(|e| AppError::DetailedIoError(format!("Failed to read file: {}", e)))??;

        let (
            journal_offset,
            tp_str,
            first_batch_queue_base_offset,
            last_batch_queue_base_offset,
            records_count,
        ) = Self::read_topic_partition(&mut buf).await;
        let topic_partition = TopicPartition::from_str(&tp_str, LogType::Queue)?;

        let memory_records = MemoryRecords::new(buf);

        Ok((
            journal_offset,
            topic_partition,
            first_batch_queue_base_offset,
            last_batch_queue_base_offset,
            records_count,
            memory_records,
        ))
    }

    async fn process_batch(
        &self,
        journal_offset: i64,
        topic_partition: TopicPartition,
        first_batch_queue_base_offset: i64,
        last_batch_queue_base_offset: i64,
        records_count: u32,
        memory_records: MemoryRecords,
    ) -> AppResult<()> {
        self.write_queue_log(
            journal_offset,
            topic_partition,
            first_batch_queue_base_offset,
            last_batch_queue_base_offset,
            records_count,
            memory_records,
        )
        .await?;

        self.journal_log.split_offset.store(journal_offset);
        trace!(
            "d,{}, 更新split offset: {}",
            records_count,
            self.journal_log.split_offset.load()
        );
        Ok(())
    }

    async fn is_active_segment(&self, base_offset: i64) -> AppResult<bool> {
        let current_active_seg_offset = self.journal_log.current_active_seg_offset();
        Ok(base_offset == current_active_seg_offset)
    }

    async fn write_queue_log(
        &self,
        journal_offset: i64,
        topic_partition: TopicPartition,
        first_batch_queue_base_offset: i64,
        last_batch_queue_base_offset: i64,
        records_count: u32,
        memory_records: MemoryRecords,
    ) -> AppResult<()> {
        return if let Some(queue_log) = self.queue_logs.get(&topic_partition) {
            queue_log
                .append_records((
                    journal_offset,
                    topic_partition,
                    first_batch_queue_base_offset,
                    last_batch_queue_base_offset,
                    records_count,
                    memory_records,
                ))
                .await?;
            Ok(())
        } else {
            Err(AppError::IllegalStateError(format!(
                "Queue log not found for topic partition: {}",
                topic_partition
            )))
        };
    }

    async fn read_topic_partition(buf: &mut BytesMut) -> (i64, String, i64, i64, u32) {
        let journal_offset = buf.get_i64();
        let tp_str_size = buf.get_u32();
        let mut tp_str_bytes = vec![0; tp_str_size as usize];
        buf.copy_to_slice(&mut tp_str_bytes);
        let first_batch_queue_base_offset = buf.get_i64();
        let last_batch_queue_base_offset = buf.get_i64();
        let records_count = buf.get_u32();
        (
            journal_offset,
            String::from_utf8(tp_str_bytes).unwrap(),
            first_batch_queue_base_offset,
            last_batch_queue_base_offset,
            records_count,
        )
    }
}
