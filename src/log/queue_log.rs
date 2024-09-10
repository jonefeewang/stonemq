use crate::log::log_segment::LogSegment;
use crate::log::Log;
use crate::message::MemoryRecords;
use crate::message::{LogAppendInfo, TopicPartition};
use crate::AppError::InvalidValue;
use crate::{global_config, AppResult};
use bytes::Buf;
use crossbeam_utils::atomic::AtomicCell;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use tokio::runtime::Runtime;
use tokio::sync::{oneshot, RwLock};
use tracing::{info, warn};

/// 每个Log代表一个topic-partition的目录及下边的日志文件
/// 日志文件以segment为单位，每个segment文件大小固定
/// 这里使用tokio的读写锁来保护并发读写：
/// 1. 写场景：roll log或delete segment时，需要修改segments，需要写锁，频率相对较低
/// 2. append message, 读取message时， 都使用的是读锁，频率相对较高
#[derive(Debug)]
pub struct QueueLog {
    pub segments: RwLock<BTreeMap<u64, LogSegment>>,
    pub dir: String,
    pub log_start_offset: u64,
    pub recover_point: u64,
}

impl Log for QueueLog {
    async fn append_records(
        &self,
        records: (TopicPartition, MemoryRecords),
    ) -> AppResult<LogAppendInfo> {
        let (topic_partition, memory_records) = records;
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
            // check again to make sure other request has rolled it already
            let mut segments = self.segments.write().await;
            let last_seg = segments.iter().next_back().unwrap();
            if *last_seg.0 == active_base_offset {
                // other request has not rolled it yet
                let new_base_offset = last_seg.1.size() as u64;
                let new_seg =
                    LogSegment::new_queue_seg(&topic_partition, new_base_offset, true).await?;
                // close old segment write, release its pipe resource
                last_seg.1.flush().await?;
                segments.insert(new_base_offset, new_seg);
            }
        }

        let segments = self.segments.read().await;
        let (active_base_offset, active_seg) = segments.iter().next_back().unwrap();
        let (tx, rx) = oneshot::channel::<AppResult<()>>();

        active_seg
            .append_record((topic_partition, memory_records, tx))
            .await?;
        rx.await??;
        Ok(LogAppendInfo {
            base_offset: *active_base_offset as i64,
            log_append_time: 0,
        })
    }
    /// 加载`dir`下的所有segment文件
    /// journal log没有index文件
    /// queue log有time index和offset index文件
    fn load_segments(topic_partition: &TopicPartition, next_offset: u64, rt: &Runtime) -> AppResult<BTreeMap<u64, LogSegment>> {
        let mut segments = BTreeMap::new();
        let dir = format!("{}/{}", global_config().log.queue_base_dir, topic_partition);
        info!("load segment files from dir:{}", topic_partition);

        if fs::read_dir(&dir)?.next().is_none() {
            info!("queue logs directory is empty: {}", &dir);
            return Ok(segments);
        }
        let mut read_dir = fs::read_dir(&dir)?;
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
                            ".timeindex" => {}
                            ".index" => {}
                            ".log" => {
                                let base_offset = file_prefix.parse::<u64>();
                                match base_offset {
                                    Ok(base_offset) => {
                                        let segment = rt.block_on(LogSegment::new_queue_seg(
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

        Ok(segments)
    }

    async fn new(
        topic_partition: &TopicPartition,
        mut segments: BTreeMap<u64, LogSegment>,
        log_start_offset: u64,
        log_recovery_point: u64,
    ) -> AppResult<Self> {
        let dir = format!("{}/{}", global_config().log.queue_base_dir, topic_partition);

        // 如果log目录不存在，先创建它
        if !Path::new::<Path>(dir.as_ref()).exists() {
            info!("log dir does not exists, create queue log dir:{}", dir);
            tokio::fs::create_dir_all(&dir).await?;
        }

        // 如果segments是空的, 默认创建一个
        // 运行时候，创建topic 不管是journal还是queue，都需要在异步运行时内，因为要做同步，在不同运行时内无法做同步
        if segments.is_empty() {
            warn!("no segment file found in queue log dir:{}", dir);
            //初始化一个空的segment
            let segment = LogSegment::new_queue_seg(topic_partition, 0, true).await?;
            segments.insert(0, segment);
        }


        Ok(QueueLog {
            dir: dir.to_string(),
            segments: RwLock::new(segments),
            log_start_offset,
            recover_point: log_recovery_point,
        })
    }

    async fn flush(&self, active_segment: &LogSegment) -> AppResult<()> {
        todo!()
    }
}
