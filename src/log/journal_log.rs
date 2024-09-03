use crate::log::log_segment::LogSegment;
use crate::log::Log;
use crate::message::MemoryRecords;
use crate::message::{LogAppendInfo, TopicPartition};
use crate::AppError::{IllegalStateError, InvalidValue};
use crate::{global_config, AppResult};
use bytes::Buf;
use crossbeam_utils::atomic::AtomicCell;
use dashmap::DashMap;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs;
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
    pub next_offset_info: DashMap<TopicPartition, i64>,
    pub dir: String,
    pub log_start_offset: u64,
    pub recover_point: AtomicCell<u64>,
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
        // assign offset for the record batch, configure the base offset,
        // set the maximum timestamp, and retain the offset of the maximum timestamp.
        // for the mvp version, we just assume the last record of the batch has the max timestamp
        let mut next_offset_value = 0i64;
        let batch_count = memory_records.records_count();
        // 修改next offset值，快速获取和释放锁
        {
            let mut next_offset = self
                .next_offset_info
                .entry(topic_partition.clone())
                .or_insert(0);
            *next_offset += batch_count as i64;
            next_offset_value = *next_offset;
        }
        // assign offset
        memory_records.set_base_offset(next_offset_value)?;
        let max_timestamp = memory_records.get_max_timestamp();
        let max_timestamp_offset = next_offset_value - 1;

        let mut active_seg_size = 0u64;
        let mut active_base_offset = 0u64;
        {
            // get active segment info
            let segments = self.segments.read().await;
            let last_seg = segments
                .iter()
                .next_back()
                .ok_or(self.no_active_segment_error(self.dir.clone()))?;
            active_seg_size = last_seg.1.size() as u64;
            active_base_offset = *last_seg.0;
            // 使用这个作用域释放读锁
        }

        let buffer = memory_records.buffer.as_ref().ok_or(InvalidValue(
            "empty message when append to file",
            topic_partition.id(),
        ))?;
        let msg_len = buffer.remaining();

        // check active segment size, if exceed the segment size, roll the segment
        if active_seg_size + msg_len as u64 >= global_config().log.journal_segment_size
        {
            // 确定需要滚动日之后，进入新的作用域，获取写锁，准备滚动日志
            let mut segments = self.segments.write().await;
            let (write_active_base, write_active_seg) = segments
                .iter()
                .next_back()
                .ok_or(self.no_active_segment_error(self.dir.clone()))?;
            // check again to make sure other request has not rolled it already
            if *write_active_base == active_base_offset {
                // other request has not rolled it yet
                let new_base_offset = write_active_seg.size() as u64;
                self.recover_point.store(new_base_offset);
                write_active_seg.flush().await?;
                write_active_seg.close_write().await;
                // create new segment file
                let new_seg =
                    LogSegment::new_queue_seg(self.dir.clone(), new_base_offset, true).await?;
                segments.insert(new_base_offset, new_seg);
                trace!(
                    "roll journal log segment to new base offset:{}",
                    new_base_offset
                );
            }
        }

        let segments = self.segments.read().await;
        let (_, active_seg) = segments
            .iter()
            .next_back()
            .ok_or(self.no_active_segment_error(self.dir.clone()))?;
        let (tx, rx) = oneshot::channel::<AppResult<()>>();

        active_seg
            .append_record((topic_partition, memory_records, tx))
            .await?;
        rx.await??;
        trace!(
            "insert {} records to journal log with offset {}",
            batch_count,
            next_offset_value
        );
        Ok(LogAppendInfo {
            base_offset: next_offset_value,
            log_append_time: 0,
        })
    }
    /// 加载`dir`下的所有segment文件
    /// journal log没有index文件
    /// queue log有time index和offset index文件
    fn load_segments(dir: &str, rt: &Runtime) -> AppResult<BTreeMap<u64, LogSegment>> {
        let mut segments = BTreeMap::new();
        info!("load segment files from dir:{}", dir);

        if fs::read_dir(dir)?.next().is_none() {
            info!("journal log directory is empty: {}", dir);
            return Ok(segments);
        }
        let mut read_dir = fs::read_dir(dir)?;
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
                                warn!("journal log should not have time index file");
                                continue;
                            }
                            ".index" => {
                                warn!("journal log should not have offset index file");
                                continue;
                            }
                            ".log" => {
                                let base_offset = file_prefix.parse::<u64>();
                                match base_offset {
                                    Ok(base_offset) => {
                                        let segment = rt.block_on(LogSegment::new_journal_seg(
                                            dir.to_string(),
                                            0,
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
        dir: String,
        mut segments: BTreeMap<u64, LogSegment>,
        log_start_offset: u64,
        log_recovery_point: u64,
    ) -> AppResult<Self> {
        // 如果segments是空的, 默认创建一个
        // 运行时候，创建topic 不管是journal还是queue，都需要在异步运行时内，因为要做同步，在不同运行时内无法做同步
        if segments.is_empty() {
            info!("no segment file found in journal log dir:{}", dir);
            //初始化一个空的segment
            let segment = LogSegment::new_journal_seg(dir.clone(), 0, true).await?;
            segments.insert(0, segment);
        }
        // 如果log目录不存在，先创建它
        if !Path::new::<Path>(dir.as_ref()).exists() {
            info!("log dir does not exists, create journal log dir:{}", dir);
            tokio::fs::create_dir_all(&dir).await?;
        }
        Ok(JournalLog {
            dir: dir.clone(),
            segments: RwLock::new(segments),
            log_start_offset,
            recover_point: AtomicCell::new(log_recovery_point),
            next_offset_info: DashMap::new(),
        })
    }

    async fn flush(&self) -> AppResult<()> {
        let segments = self.segments.read().await;
        let (_, active_seg) = segments.iter().next_back().ok_or_else(|| {
            IllegalStateError(Cow::Owned(format!(
                "no active segment found in journal log:{}",
                self.dir.clone(),
            )))
        })?;
        let size = active_seg.flush().await?;
        self.recover_point.store(size);
        Ok(())
    }
}
