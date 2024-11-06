mod read;

use crate::log::file_records::FileRecords;
use crate::log::index_file::IndexFile;
use crate::log::log_segment::LogSegment;
use crate::log::Log;
use crate::message::{LogAppendInfo, MemoryRecords, TopicPartition};
use crate::{global_config, AppError, AppResult};
use crossbeam::atomic::AtomicCell;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::path::Path;
use tokio::runtime::Runtime;
use tokio::sync::{oneshot, RwLock};
use tracing::{error, info, trace, warn};

use super::{LogType, PositionInfo};

#[derive(Debug)]
pub struct QueueLog {
    pub segments: RwLock<BTreeMap<i64, LogSegment>>,
    pub topic_partition: TopicPartition,
    pub log_start_offset: i64,
    pub recover_point: AtomicCell<i64>,
    pub last_offset: AtomicCell<i64>,
    pub index_file_max_size: u32,
}

impl Hash for QueueLog {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic_partition.hash(state);
    }
}

impl PartialEq for QueueLog {
    fn eq(&self, other: &Self) -> bool {
        self.topic_partition == other.topic_partition
    }
}

impl Eq for QueueLog {}

impl Log for QueueLog {
    /// Appends records to the queue log.
    ///
    /// # Arguments
    ///
    /// * `records` - A tuple containing the topic partition and memory records to append.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing `LogAppendInfo` on success.
    ///
    /// # Performance
    ///
    /// This method acquires a write lock on the entire log, which may impact concurrent operations.
    async fn append_records(
        &self,
        records: (TopicPartition, i64, MemoryRecords),
    ) -> AppResult<LogAppendInfo> {
        let (topic_partition, offset, memory_records) = records;

        trace!(
            "append records to queue log:{}, offset:{}, records_count:{}",
            &topic_partition,
            offset,
            memory_records.records_count()
        );

        let (active_seg_size, active_segment_offset_index_full) = {
            let segments = self.segments.read().await;
            let active_seg = segments
                .iter()
                .next_back()
                .ok_or_else(|| self.no_active_segment_error(&self.topic_partition))?;
            (
                active_seg.1.size() as u32,
                active_seg.1.offset_index_full().await?,
            )
        };

        let need_roll = self
            .need_roll(
                active_seg_size,
                &memory_records,
                active_segment_offset_index_full,
            )
            .await;

        if need_roll {
            self.roll_segment().await?;
        }

        let log_append_info = LogAppendInfo {
            base_offset: memory_records.base_offset(),
            log_append_time: -1,
        };
        let last_offset_delta = memory_records.last_offset_delta();

        self.append_to_active_segment(topic_partition, offset, memory_records)
            .await?;

        self.last_offset
            .store(log_append_info.base_offset + last_offset_delta as i64);
        trace!(
            "append records to queue log success, update last offset:{}",
            self.last_offset.load()
        );

        Ok(log_append_info)
    }
}

impl QueueLog {
    /// Creates a new QueueLog instance.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition for this log.
    /// * `segments` - A BTreeMap of existing log segments.
    /// * `log_start_offset` - The starting offset for this log.
    /// * `recovery_offset` - The recovery point offset.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the new `QueueLog` instance on success.
    pub async fn new(
        topic_partition: &TopicPartition,
        mut segments: BTreeMap<i64, LogSegment>,
        log_start_offset: i64,
        recovery_offset: i64,
        next_offset: i64,
        index_file_max_size: u32,
    ) -> AppResult<Self> {
        let dir = topic_partition.queue_partition_dir();

        if !Path::new(&dir).exists() {
            info!("log dir does not exists, create queue log dir:{}", dir);
            std::fs::create_dir_all(&dir)?;
        }

        if segments.is_empty() {
            warn!("no segment file found in queue log dir:{}", dir);
            let segment = LogSegment::new(topic_partition, &dir, 0, index_file_max_size).await?;
            segments.insert(0, segment);
        }

        Ok(QueueLog {
            topic_partition: topic_partition.clone(),
            segments: RwLock::new(segments),
            log_start_offset,
            recover_point: AtomicCell::new(recovery_offset),
            last_offset: AtomicCell::new(next_offset),
            index_file_max_size,
        })
    }

    /// Flushes the active segment to disk.
    ///
    /// # Arguments
    ///
    /// * `active_segment` - The active LogSegment to flush.
    ///
    /// # Returns
    ///
    /// Returns `AppResult<()>` indicating success or failure.
    pub async fn flush(&self) -> AppResult<()> {
        let segments = self.segments.write().await;
        let active_seg = segments
            .iter()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error(&self.topic_partition))?;
        active_seg.1.flush().await?;
        self.recover_point.store(self.last_offset.load());
        Ok(())
    }

    /// Loads segments for a given topic partition.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition to load segments for.
    /// * `next_offset` - The next offset to use.
    /// * `rt` - The runtime to use for blocking operations.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a `BTreeMap` of base offsets to `LogSegment`s.
    pub fn load_segments(
        topic_partition: &TopicPartition,
        _: i64,
        index_file_max_size: u32,
        rt: &Runtime,
    ) -> AppResult<BTreeMap<i64, LogSegment>> {
        let mut segments = BTreeMap::new();
        let dir = topic_partition.queue_partition_dir();
        info!("从目录加载段文件：{}", dir);

        if std::fs::read_dir(&dir)?.next().is_none() {
            info!("队列日志目录为空：{}", &dir);
            return Ok(segments);
        }

        let mut index_files = BTreeMap::new();
        let mut log_files = BTreeMap::new();

        let mut read_dir = std::fs::read_dir(&dir)?;
        while let Some(file) = read_dir.next().transpose()? {
            if file.metadata()?.file_type().is_file() {
                let file_name = file.file_name().to_string_lossy().to_string();
                let dot_index = file_name.rfind('.');
                match dot_index {
                    None => {
                        warn!("无效的段文件名：{}", file_name);
                        continue;
                    }
                    Some(dot_index) => {
                        let file_prefix = &file_name[..dot_index];
                        let file_suffix = &file_name[dot_index..];
                        match file_suffix {
                            ".index" => {
                                let index_file = rt.block_on(IndexFile::new(
                                    file.path(),
                                    index_file_max_size as usize,
                                    false,
                                ))?;
                                index_files.insert(file_prefix.parse::<i64>()?, index_file);
                            }
                            ".log" => {
                                // TODO: 修复还未flush到磁盘上的数据，就是recovery_point到log最后的一个offset数据
                                // 这里目前只覆盖优雅关闭的场景
                                let base_offset = file_prefix.parse::<i64>();
                                match base_offset {
                                    Ok(base_offset) => {
                                        let file_records =
                                            rt.block_on(FileRecords::open(file.path()))?;
                                        log_files.insert(base_offset, file_records);
                                    }
                                    Err(_) => {
                                        warn!("无效的段文件名：{}", file_prefix);
                                        continue;
                                    }
                                }
                            }
                            other => {
                                warn!("无效的段文件名：{}", other);
                                continue;
                            }
                        }
                    }
                }
            }
        }

        for (base_offset, file_records) in log_files {
            let offset_index = index_files.remove(&base_offset);
            if let Some(offset_index) = offset_index {
                let segment = LogSegment::open(
                    topic_partition.clone(),
                    base_offset,
                    file_records,
                    offset_index,
                    None,
                );
                segments.insert(base_offset, segment);
            } else {
                error!("未找到偏移索引文件：{}", base_offset);
            }
        }

        Ok(segments)
    }

    /// 从已有的数据加载 `QueueLog`。
    ///
    /// # 参数
    ///
    /// * `topic_partition` - 主题分区。
    /// * `recover_point` - 恢复点偏移量。
    /// * `dir` - 数据目录。
    /// * `max_index_file_size` - 最大索引文件大小。
    /// * `rt` - Tokio 运行时。
    ///
    /// # 返回
    ///
    /// 返回加载的 `QueueLog` 实例。
    pub fn load_from(
        topic_partition: &TopicPartition,
        recover_point: i64,
        index_file_max_size: u32,
        rt: &Runtime,
    ) -> AppResult<Self> {
        let segments =
            Self::load_segments(topic_partition, recover_point, index_file_max_size, rt)?;

        let log_start_offset = segments
            .first_key_value()
            .map(|(offset, _)| *offset)
            .unwrap_or(0);

        let log = QueueLog {
            topic_partition: topic_partition.clone(),
            segments: RwLock::new(segments),
            log_start_offset,
            recover_point: AtomicCell::new(recover_point),
            last_offset: AtomicCell::new(recover_point),
            index_file_max_size,
        };
        Ok(log)
    }

    /// Determines if a new segment roll is needed.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition.
    /// * `active_seg_size` - The size of the active segment.
    /// * `memory_records` - The memory records to be appended.
    /// * `active_segment_offset_index_full` - Whether the active segment's offset index is full.
    ///
    /// # Returns
    ///
    /// Returns a boolean indicating whether a new segment roll is needed.
    async fn need_roll(
        &self,
        active_seg_size: u32,
        memory_records: &MemoryRecords,
        active_segment_offset_index_full: bool,
    ) -> bool {
        active_seg_size + memory_records.size() as u32
            >= global_config().log.queue_segment_size as u32
            || active_segment_offset_index_full
    }

    /// Rolls over to a new log segment.
    ///
    /// # Returns
    ///
    /// Returns `AppResult<()>` indicating success or failure.
    ///
    /// # Performance
    ///
    /// This method acquires a write lock on the segments, which may impact concurrent operations.
    async fn roll_segment(&self) -> AppResult<()> {
        let mut segments = self.segments.write().await;
        let (_, active_seg) = segments
            .iter()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error(&self.topic_partition))?;

        active_seg.flush().await?;
        self.recover_point.store(self.last_offset.load());

        let new_base_offset = self.last_offset.load() + 1;

        let new_seg = LogSegment::new(
            &self.topic_partition,
            self.topic_partition.queue_partition_dir(),
            new_base_offset,
            self.index_file_max_size,
        )
        .await?;
        segments.insert(new_base_offset, new_seg);
        trace!(
            "Rolled queue log segment to new base offset: {}",
            new_base_offset
        );
        Ok(())
    }

    /// Appends records to the active segment.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition.
    /// * `memory_records` - The memory records to append.
    ///
    /// # Returns
    ///
    /// Returns `AppResult<()>` indicating success or failure.
    ///
    /// # Performance
    ///
    /// This method acquires a read lock on the segments, which may impact concurrent operations.
    async fn append_to_active_segment(
        &self,
        topic_partition: TopicPartition,
        offset: i64,
        memory_records: MemoryRecords,
    ) -> AppResult<()> {
        let segments = self.segments.read().await;
        let active_seg = segments
            .iter()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error(&self.topic_partition))?;

        let (tx, rx) = oneshot::channel();
        active_seg
            .1
            .append_record(
                LogType::Queue,
                (offset, topic_partition, memory_records, tx),
            )
            .await?;
        rx.await??;
        Ok(())
    }

    /// Creates an error for when no active segment is found.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition for which no active segment was found.
    ///
    /// # Returns
    ///
    /// Returns an `AppError` describing the error condition.
    fn no_active_segment_error(&self, topic_partition: &TopicPartition) -> AppError {
        AppError::CommonError(format!(
            "No active segment for topic partition: {}",
            topic_partition
        ))
    }

    pub async fn get_leo_info(&self) -> AppResult<PositionInfo> {
        let segments = self.segments.read().await;
        let (base_offset, active_seg) = segments
            .iter()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error(&self.topic_partition))?;
        let leo_info = PositionInfo {
            base_offset: *base_offset,
            offset: self.last_offset.load(),
            position: active_seg.size() as i64,
        };
        Ok(leo_info)
    }
}
