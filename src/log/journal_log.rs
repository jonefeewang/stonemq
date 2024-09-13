use crate::log::log_segment::{LogSegment, PositionInfo};
use crate::log::Log;
use crate::message::MemoryRecords;
use crate::message::{LogAppendInfo, TopicPartition};
use crate::AppError::{CommonError, InvalidValue};
use crate::{global_config, AppResult};
use crossbeam_utils::atomic::AtomicCell;
use dashmap::DashMap;
use std::collections::BTreeMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::Path;
use tokio::runtime::Runtime;
use tokio::sync::{oneshot, RwLock};
use tracing::{info, trace, warn};

/// 1. 写场景：roll log或delete segment时，需要修改segments，需要写锁，频率相对较低
/// 2. append message, 读取message时， 都使用的是读锁，频率相对较高
#[derive(Debug)]
pub struct JournalLog {
    pub segments: RwLock<BTreeMap<u64, LogSegment>>,
    // queue log的topicPartition对应的下一个offset值
    pub queue_next_offset_info: DashMap<TopicPartition, i64>,
    pub log_start_offset: u64,
    // journal log's next offset
    pub next_offset: AtomicCell<u64>,
    // offset
    pub recover_point: AtomicCell<u64>,
    // split point offset
    pub split_offset: AtomicCell<u64>,
    // journal log的全局锁，确保插入消息等操作的一致性
    write_lock: RwLock<()>,
    // journal log的topicPartition
    topic_partition: TopicPartition,
}
impl Hash for JournalLog {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic_partition.hash(state);
    }
}

impl Log for JournalLog {
    ///
    /// 1. validate memory records
    /// 2. assign offset to records
    /// 2. may be rolled the segment file
    /// 3. append to active segment file
    /// 注意：这里的records(TopicPartition, MemoryRecords)是queue log的topicPartition
    async fn append_records(
        &self,
        records: (TopicPartition, MemoryRecords),
    ) -> AppResult<LogAppendInfo> {
        let (topic_partition, mut memory_records) = records;
        // 0. 校验消息可以在外部被并发执行

        // 1.开始插入消息，这时需要一全局的锁，next offset的修改，segment的滚动,以及recover point的修改 保持一致性
        {
            let _ = self.write_lock.write().await;

            // 1.先拿segments的读锁获取相关信息
            let mut active_seg_size = 0u64;
            let mut active_segment_offset_index_full = false;
            {
                // get active segment info
                let segments = self.segments.read().await;
                let (_, active_seg) = segments
                    .iter()
                    .next_back()
                    .ok_or(self.no_active_segment_error(&self.topic_partition))?;
                active_seg_size = active_seg.size() as u64;
                active_segment_offset_index_full = active_seg.offset_index_full().await?;
                // 快速释放segments的读锁，防止阻塞读消息的线程
            }

            // 2. 计算需要滚动的条件
            // size + journal log offset + current segment size + memory_records.size()
            let mut need_roll = 4 + 8 + active_seg_size + memory_records.size() as u64 >= global_config().log.journal_segment_size
                || active_segment_offset_index_full;

            // 3.如果需要的话，再拿整个segments的写锁做实际滚动
            if need_roll
            {
                // 确定需要滚动日之后，进入新的作用域，获取写锁，准备滚动日志
                let mut segments = self.segments.write().await;
                let (_, write_active_seg) = segments
                    .iter()
                    .next_back()
                    .ok_or(self.no_active_segment_error(&self.topic_partition))?;

                self.flush(write_active_seg).await?;
                let new_base_offset = self.next_offset.load();

                // create new segment file
                let new_seg =
                    LogSegment::new_journal_seg(&self.topic_partition, new_base_offset, true).await?;
                segments.insert(new_base_offset, new_seg);
                trace!(
                    "roll journal log segment to new base offset:{}",
                    new_base_offset
                );
            }
            // 4. 拿到active segment后，开始assign offset，
            let mut queue_log_next_offset_value = 0i64;
            let batch_count = memory_records.records_count();

            {
                // 修改next offset值，快速获取和释放queue_next_offset_info的锁
                let mut next_offset = self
                    .queue_next_offset_info
                    .entry(topic_partition.clone())
                    .or_insert(0);
                queue_log_next_offset_value = *next_offset;
            }
            memory_records.set_base_offset(queue_log_next_offset_value)?;

            // 5. 插入消息，插入完成后，更新next offset
            {
                // 获取segments的读锁，快速释放
                let segments = self.segments.read().await;
                let (_, active_seg) = segments
                    .iter()
                    .next_back()
                    .ok_or(self.no_active_segment_error(&self.topic_partition))?;
                let (tx, rx) = oneshot::channel::<AppResult<()>>();

                active_seg
                    .append_record((topic_partition.clone(), memory_records, tx))
                    .await?;
                rx.await??;
                // 快速释放segments的读锁，防止阻塞读消息的线程
            }

            trace!("insert {} records to journal log with offset {}",batch_count,self.next_offset.load());

            // 6.更新queue log的next offset
            {
                // 修改next offset值，快速获取和释放queue_next_offset_info的锁
                let mut next_offset = self
                    .queue_next_offset_info
                    .entry(topic_partition.clone())
                    .or_insert(0);
                *next_offset += batch_count as i64;
                queue_log_next_offset_value = *next_offset;
            }

            // 7. 更新journal log next offset
            self.next_offset.fetch_add(batch_count as u64);

            Ok(LogAppendInfo {
                base_offset: queue_log_next_offset_value,
                log_append_time: 0,
            })

            // 8. 释放全局锁
        }
    }
    /// 加载`dir`下的所有segment文件
    /// journal log没有index文件
    /// queue log有time index和offset index文件
    fn load_segments(topic_partition: &TopicPartition, next_offset: u64, rt: &Runtime) -> AppResult<BTreeMap<u64, LogSegment>> {
        let mut segments = BTreeMap::new();
        info!("load segment files from topic partition:{}", topic_partition);

        let partition_dir = format!("{}/{}", global_config().log.journal_base_dir, topic_partition);
        let mut read_dir = fs::read_dir(partition_dir)?;
        while let Some(file) = read_dir.next().transpose()? {
            if file.metadata()?.file_type().is_file() {
                let file_name = file.file_name().to_string_lossy().to_string();
                let dot_index = file_name.rfind('.');
                match dot_index {
                    None => {
                        warn!("invalid segment file name:{}", file_name);
                        continue;
                    }
                    Some(dot_index) => {
                        let file_prefix = &file_name[..dot_index];
                        let file_suffix = &file_name[dot_index..];
                        match file_suffix {
                            ".timeindex" => {
                                //warn!("journal log should not have time index file");
                                continue;
                            }
                            ".index" => {
                                //warn!("journal log should not have offset index file");
                                continue;
                            }
                            ".log" => {
                                let base_offset = file_prefix.parse::<u64>();
                                match base_offset {
                                    Ok(base_offset) => {
                                        let segment = rt.block_on(LogSegment::new_journal_seg(
                                            topic_partition,
                                            base_offset,
                                            false,
                                        ))?;
                                        segments.insert(base_offset, segment);
                                    }
                                    Err(_) => {
                                        warn!("invalid segment file name:{}", file_prefix);
                                        continue;
                                    }
                                }
                            }
                            other => {
                                warn!("invalid segment file name:{}", other);
                                continue;
                            }
                        }
                    }
                }
            }
        }
        // 初始化

        Ok(segments)
    }

    async fn new(
        topic_partition: &TopicPartition,
        mut segments: BTreeMap<u64, LogSegment>,
        log_start_offset: u64,
        log_recovery_point: u64,
        split_offset: u64,
    ) -> AppResult<Self> {

        // 如果log目录不存在，先创建它
        let dir = format!("{}/{}", global_config().log.journal_base_dir, topic_partition);
        if !Path::new::<Path>(dir.as_ref()).exists() {
            info!("log dir does not exists, create journal log dir:{}", dir);
            tokio::fs::create_dir_all(&dir).await?;
        }

        // 如果segments是空的, 默认创建一个
        // 运行时候，创建topic 不管是journal还是queue，都需要在异步运行时内，因为要做同步，在不同运行时内无法做同步
        if segments.is_empty() {
            info!("no segment file found in journal log dir:{}", dir);
            //初始化一个空的segment
            let segment = LogSegment::new_journal_seg(topic_partition, 0, true).await?;
            segments.insert(0, segment);
        }


        // stoneMQ 假设目前关闭都是优雅关闭的，那么journal log关闭时会flush，最后flush生成recovery point中包含了next offset

        Ok(JournalLog {
            topic_partition: topic_partition.clone(),
            segments: RwLock::new(segments),
            log_start_offset,
            next_offset: AtomicCell::new(log_recovery_point),
            queue_next_offset_info: DashMap::new(),
            write_lock: RwLock::new(()),
            recover_point: AtomicCell::new(log_recovery_point),
            split_offset: AtomicCell::new(split_offset),
        })
    }

    /// Flushes the active segment of the journal log and updates the recovery point.
    ///
    /// This method is responsible for flushing the active segment of the journal log to disk,
    /// and then updating the recovery point with the current next offset and the size of the
    /// flushed segment. This ensures that the journal log can be properly recovered from the
    /// last known state in the event of a system failure or restart.
    ///
    ///
    /// # Returns
    /// A `Result` containing `()` on success, or an error if the flush or recovery point
    /// update fails.
    /// # Notes
    /// 注意锁的使用，防止死锁
    async fn flush(&self, active_segment: &LogSegment) -> AppResult<()> {
        active_segment.flush().await?;
        self.recover_point.store(self.next_offset.load());
        Ok(())
    }
}
impl JournalLog {
    pub async fn get_position_info(&self, offset: u64) -> AppResult<PositionInfo> {
        // 使用到segments的读锁
        let segments = self.segments.read().await;
        let segment = segments
            .range(..=offset)
            .next_back()
            .map(|(_, segment)| segment)
            .ok_or_else(|| CommonError(format!("No segment found for offset {}", offset)))?;
        segment.get_position(offset).await
    }
    pub async fn current_active_seg_offset(&self) -> u64 {
        // 获取segments的读锁，快速释放
        let segments = self.segments.read().await;
        let (_, active_seg) = segments
            .iter()
            .next_back()
            .ok_or(self.no_active_segment_error(&self.topic_partition)).unwrap();
        active_seg.base_offset()
    }
    pub async fn next_base_offset(&self, base_offset: u64) -> u64 {
        todo!()
    }
}
