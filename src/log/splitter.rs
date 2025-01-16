use crate::log::{JournalLog, LogType};
use crate::message::{MemoryRecords, TopicPartition};
use crate::{global_config, AppError, AppResult, Shutdown};
use bytes::{Buf, BytesMut};

use std::collections::BTreeMap;
use std::error::Error;
use std::fs::File;
use std::io::{self, ErrorKind, Read};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::time::Interval;
use tracing::{debug, error, instrument, trace};

use super::queue_log::QueueLog;
use super::JournalRecordsBatch;

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
            let target_offset = self.journal_log.split_offset.load(Ordering::Acquire) + 1;
            debug!(
                "start loop and read target offset: {} / {}",
                target_offset, &self.topic_partition
            );

            match self.read_from(target_offset, &mut shutdown).await {
                // 正常读取和处理，一直读取到段末尾EOF，本段处理完成，返回Ok(())，等待切换到下一个段
                Ok(()) => continue,
                Err(e) if self.is_retrievable_error(&e) => {
                    trace!(
                        "读取目标offset失败,可恢复中: {}/{}",
                        e,
                        &self.topic_partition
                    );
                    if shutdown.is_shutdown() {
                        return Ok(());
                    }
                    tokio::select! {
                        _ = self.read_wait_interval.tick() => {
                            trace!("等待继续读取...... / {}", &self.topic_partition);
                        },
                        _ = shutdown.recv() => {
                            return Ok(());
                        }
                    }
                    continue;
                }
                Err(e) => {
                    error!(
                        "读取目标offset失败,不no no可恢复: {}/{}",
                        e, &self.topic_partition
                    );
                    return Err(e);
                }
            }
        }
        debug!("splitter task exit");
        Ok(())
    }

    fn is_retrievable_error(&self, e: &AppError) -> bool {
        trace!("is_retrievable_error: {:?}/{}", e, &self.topic_partition);
        e.source()
            .and_then(|e| e.downcast_ref::<std::io::Error>())
            .map_or(false, |std_err| {
                matches!(
                    std_err.kind(),
                    ErrorKind::NotFound | ErrorKind::UnexpectedEof
                )
            })
    }

    // 只要将文件指针seek到目标offset，然后一直读取到段末尾EOF，本段处理完成，返回Ok(()),读取过程中出现的Eof错误,自己处理
    // 函数返回值:
    //   1. 正常读取和处理，一直读取到段末尾EOF，本段处理完成，返回Ok(())，等待切换到下一个段
    //   2. get_relative_position_info返回not found,或seek_file返回NotFound或Eof错误,File::open返回NotFound，返回Err(e)，这种错误需要在下游重试
    //   3. 其他文件错误，返回Err(e)，不可恢复错误，下游应该明确抛出，并退出
    async fn read_from(&mut self, target_offset: i64, shutdown: &mut Shutdown) -> AppResult<()> {
        let refer_position_info = self.journal_log.get_relative_position_info(target_offset)?;

        let journal_topic_dir = PathBuf::from(global_config().log.journal_base_dir.clone())
            .join(self.topic_partition.id());
        let segment_path =
            journal_topic_dir.join(format!("{}.log", refer_position_info.base_offset));
        let journal_seg_file = std::fs::File::open(&segment_path)?;

        debug!(
            "开始读取一个segment{:?} ref:  {:?}, target offset: {} / {}",
            segment_path, &refer_position_info, target_offset, &self.topic_partition
        );

        let (file, exact_position) = crate::log::seek_file(
            journal_seg_file,
            target_offset,
            refer_position_info,
            LogType::Journal,
        )
        .await?;

        trace!(
            "文件内部读指针位置: {}/{}",
            exact_position.position,
            &self.topic_partition
        );

        loop {
            if shutdown.is_shutdown() {
                return Ok(());
            }
            let file_clone = file.try_clone()?;
            let read_result = tokio::select! {
                read_result = self.read_batch(file_clone) => read_result,
                _ = shutdown.recv() => {
                    return Ok(());
                }
            };

            match read_result {
                Ok(journal_records_batch) => {
                    // successfully read a batch from journal log, append to the corresponding queue log
                    let records_count = journal_records_batch.records_count;
                    let journal_offset = journal_records_batch.journal_offset;
                    self.queue_logs
                        .get(&journal_records_batch.queue_topic_partition)
                        .unwrap()
                        .append_records(journal_records_batch)
                        .await?;

                    self.journal_log
                        .split_offset
                        .store(journal_offset, Ordering::Release);
                    trace!(
                        "process batch,{}, update split offset: {}/{}",
                        records_count,
                        self.journal_log.split_offset.load(Ordering::Acquire),
                        &self.topic_partition
                    );
                    continue;
                }
                Err(e) => {
                    // read batch from journal log failed
                    if let Some(io_err) =
                        e.source().and_then(|e| e.downcast_ref::<std::io::Error>())
                    {
                        trace!("io_err: {:?}/{}", io_err, &self.topic_partition);
                        if io_err.kind() == ErrorKind::UnexpectedEof {
                            // 如果读到EOF，原因只有两种
                            // 1. 历史段，已经读取完毕
                            // 2. 活动段，数据暂时还读不到(可能是因为数据还在写入)，活动段确实已经读取到结尾
                            if self.is_active_segment(exact_position.base_offset)? {
                                if shutdown.is_shutdown() {
                                    return Ok(());
                                }
                                trace!(
                                    "current segment is active segment, wait to retry / {}",
                                    &self.topic_partition
                                );
                                tokio::select! {
                                    _ = self.read_wait_interval.tick() => {},
                                    _ = shutdown.recv() => return Ok(()),
                                }
                                continue;
                            } else {
                                // if current segment is not active segment,
                                // return and switch to next segment
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

    fn is_active_segment(&self, base_offset: i64) -> AppResult<bool> {
        let current_active_seg_offset = self.journal_log.current_active_seg_offset();
        Ok(base_offset == current_active_seg_offset)
    }

    async fn read_batch(&self, mut file: File) -> AppResult<JournalRecordsBatch> {
        // read a batch from journal log
        let mut buf = tokio::task::spawn_blocking({
            move || -> io::Result<BytesMut> {
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

        // Parse the buffer and create a `JournalRecordsBatch`.
        let journal_offset = buf.get_i64();
        let tp_str_size = buf.get_u32();
        // queue topic partition string
        let mut tp_str_bytes = vec![0; tp_str_size as usize];
        buf.copy_to_slice(&mut tp_str_bytes);
        let queue_topic_partition_str = String::from_utf8(tp_str_bytes).unwrap();
        let queue_topic_partition =
            TopicPartition::from_str(&queue_topic_partition_str, LogType::Queue)?;
        let first_batch_queue_base_offset = buf.get_i64();
        let last_batch_queue_base_offset = buf.get_i64();
        let records_count = buf.get_u32();
        let memory_records = MemoryRecords::new(buf);

        Ok(JournalRecordsBatch {
            journal_offset,
            queue_topic_partition,
            first_batch_queue_base_offset,
            last_batch_queue_base_offset,
            records_count,
            records: memory_records,
        })
    }
}
