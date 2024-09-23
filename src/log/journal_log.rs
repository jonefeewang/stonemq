use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use crossbeam_utils::atomic::AtomicCell;
use dashmap::DashMap;
use tokio::runtime::Runtime;
use tokio::sync::{oneshot, RwLock};
use tracing::{error, info, trace, warn};

use super::file_records::FileRecords;
use super::index_file::IndexFile;
use super::log_segment::PositionInfo;
use super::{
    calculate_journal_log_overhead, CheckPointFile, Log, NEXT_OFFSET_CHECKPOINT_FILE_NAME,
};
use crate::log::log_segment::LogSegment;
use crate::message::{LogAppendInfo, MemoryRecords, TopicPartition};
use crate::AppError::{self, CommonError};
use crate::{global_config, AppResult};

/// 表示日志分区的日志管理器。
///
/// 负责管理日志段、追加记录以及滚动日志段。
#[derive(Debug)]
pub struct JournalLog {
    /// 日志段的有序映射，使用 `RwLock` 以允许多个并发读取和单一写入。
    segments: RwLock<BTreeMap<u64, Arc<LogSegment>>>,

    /// 队列下一个偏移信息，使用 `DashMap` 以提供并发安全的哈希映射。
    queue_next_offset_info: DashMap<TopicPartition, i64>,

    /// 日志开始偏移量。
    log_start_offset: AtomicCell<u64>,

    /// 下一个偏移量。
    next_offset: AtomicCell<u64>,

    /// 恢复点偏移量。
    pub(crate) recover_point: AtomicCell<u64>,

    /// 分割偏移量。
    pub split_offset: AtomicCell<u64>,

    /// 写操作的锁。
    write_lock: RwLock<()>,

    /// 主题分区信息。
    topic_partition: TopicPartition,

    /// 最大索引文件大小。
    index_file_max_size: u32,

    queue_next_offset_checkpoints: CheckPointFile,
}

impl Log for JournalLog {
    /// 追加记录到日志。
    ///
    /// # 参数
    ///
    /// * `records` - 包含主题分区和要追加的内存记录的元组。
    ///
    /// # 返回
    ///
    /// 返回包含追加操作信息的 `AppResult<LogAppendInfo>`。
    async fn append_records(
        &self,
        records: (TopicPartition, u64, MemoryRecords),
    ) -> AppResult<LogAppendInfo> {
        let (topic_partition, _, mut memory_records) = records;
        let _write_guard = self.write_lock.write().await; // 获取写锁以进行原子追加操作

        let (active_seg_size, active_segment_offset_index_full) =
            self.get_active_segment_info().await?;

        if self
            .need_roll(
                &topic_partition,
                active_seg_size,
                &memory_records,
                active_segment_offset_index_full,
            )
            .await
        {
            self.roll_segment().await?;
        }

        let records_count = memory_records.records_count() as u32;

        self.assign_offset(&topic_partition, &mut memory_records)
            .await?;
        self.append_to_active_segment(topic_partition, memory_records)
            .await?;

        let log_append_info = LogAppendInfo {
            base_offset: self.next_offset.load() as i64,
            log_append_time: -1,
        };

        self.update_offsets(records_count).await;

        Ok(log_append_info)
    }

    /// 提供默认的活动段错误信息。
    fn no_active_segment_error(&self, topic_partition: &TopicPartition) -> AppError {
        CommonError(format!("未找到活动段，日志分区: {}", topic_partition))
    }
}

impl JournalLog {
    /// 创建一个新的 `JournalLog` 实例。
    ///
    /// # 参数
    ///
    /// * `topic_partition` - 此日志的主题分区。
    /// * `segments` - 现有日志段的有序映射。
    /// * `log_start_offset` - 此日志的起始偏移量。
    /// * `log_recovery_point` - 恢复点偏移量。
    /// * `split_offset` - 分割偏移量。
    /// * `max_index_file_size` - 最大索引文件大小。
    ///
    /// # 返回
    ///
    /// 返回包含新 `JournalLog` 实例的 `AppResult<Self>`。
    pub async fn new(
        topic_partition: TopicPartition,
        segments: BTreeMap<u64, Arc<LogSegment>>,
        log_start_offset: u64,
        log_recovery_point: u64,
        split_offset: u64,
        index_file_max_size: u32,
    ) -> AppResult<Self> {
        let dir = topic_partition.journal_partition_dir();
        tokio::fs::create_dir_all(&dir).await?;

        let segments = if segments.is_empty() {
            info!("日志目录中未找到段文件: {}", dir);
            let initial_log_file_name = format!("{}/0.log", dir);
            let initial_index_file_name = format!("{}/0.index", dir);
            let segment = Arc::new(LogSegment::open(
                topic_partition.clone(),
                0,
                FileRecords::open(initial_log_file_name).await?,
                IndexFile::new(initial_index_file_name, index_file_max_size as usize, false)
                    .await?,
                None,
            ));
            let mut segments = BTreeMap::new();
            segments.insert(0, segment);
            segments
        } else {
            segments
        };

        Ok(Self {
            segments: RwLock::new(segments),
            queue_next_offset_info: DashMap::new(),
            queue_next_offset_checkpoints: CheckPointFile::new(format!(
                "{}/{}",
                &topic_partition.journal_partition_dir(),
                NEXT_OFFSET_CHECKPOINT_FILE_NAME
            )),
            log_start_offset: AtomicCell::new(log_start_offset),
            next_offset: AtomicCell::new(log_recovery_point + 1),
            recover_point: AtomicCell::new(log_recovery_point),
            split_offset: AtomicCell::new(split_offset),
            write_lock: RwLock::new(()),
            topic_partition,
            index_file_max_size,
        })
    }

    /// 将活动段刷新到磁盘。
    ///
    /// # 参数
    ///
    /// * `active_segment` - 要刷新的活动 `LogSegment`。
    ///
    /// # 返回
    ///
    /// 返回 `AppResult<()>` 表示成功或失败。
    pub async fn flush(&self, active_segment: &LogSegment) -> AppResult<()> {
        active_segment.flush().await?;
        self.recover_point.store(self.next_offset.load());
        Ok(())
    }

    /// 获取给定偏移量的位置信息。
    ///
    /// # 参数
    ///
    /// * `offset` - 要获取位置信息的偏移量。
    ///
    /// # 返回
    ///
    /// 返回包含位置信息的 `AppResult<PositionInfo>`。
    pub async fn get_position_info(&self, offset: u64) -> AppResult<PositionInfo> {
        let segments = self.segments.read().await; // 获取读锁以进行并发读取
        let segment = segments
            .range(..=offset)
            .next_back()
            .map(|(_, segment)| segment)
            .ok_or_else(|| CommonError(format!("未找到偏移量 {} 的段", offset)))?;
        segment.get_position(offset).await
    }

    /// 获取当前活动段的偏移量。
    ///
    /// # 返回
    ///
    /// 返回包含活动段偏移量的 `AppResult<u64>`。
    pub async fn current_active_seg_offset(&self) -> AppResult<u64> {
        let segments = self.segments.read().await; // 获取读锁以进行并发读取
        let active_seg = segments
            .values()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error())?;
        Ok(active_seg.base_offset())
    }

    /// 获取活动段的信息。
    ///
    /// # 返回
    ///
    /// 返回包含段大小和偏移索引是否已满的 `AppResult<(u32, bool)>`。
    async fn get_active_segment_info(&self) -> AppResult<(u32, bool)> {
        let segments = self.segments.read().await; // 获取读锁以进行并发读取
        let active_seg = segments
            .values()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error())?;
        Ok((
            active_seg.size() as u32,
            active_seg.offset_index_full().await?,
        ))
    }

    /// 判断是否需要滚动到新的日志段。
    ///
    /// # 参数
    ///
    /// * `topic_partition` - 主题分区。
    /// * `active_seg_size` - 活动段的大小。
    /// * `memory_records` - 要追加的内存记录。
    /// * `active_segment_offset_index_full` - 活动段的偏移索引是否已满。
    ///
    /// # 返回
    ///
    /// 返回一个布尔值，指示是否需要滚动。
    async fn need_roll(
        &self,
        topic_partition: &TopicPartition,
        active_seg_size: u32,
        memory_records: &MemoryRecords,
        active_segment_offset_index_full: bool,
    ) -> bool {
        let config = &global_config().log;
        let total_size = calculate_journal_log_overhead(topic_partition)
            + memory_records.size() as u32
            + active_seg_size;

        total_size >= config.journal_segment_size as u32 || active_segment_offset_index_full
    }

    /// 滚动到新的日志段。
    ///
    /// # 返回
    ///
    /// 返回 `AppResult<()>` 表示成功或失败。
    async fn roll_segment(&self) -> AppResult<()> {
        let mut segments = self.segments.write().await; // 获取写锁以进行原子滚动操作
        let (_, active_seg) = segments
            .iter()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error())?;

        self.flush(active_seg).await?;
        let new_base_offset = self.next_offset.load();

        let new_seg = Arc::new(LogSegment::open(
            self.topic_partition.clone(),
            new_base_offset,
            FileRecords::open(format!(
                "{}/{}.log",
                self.topic_partition.journal_partition_dir(),
                new_base_offset
            ))
            .await?,
            IndexFile::new(
                format!(
                    "{}/{}",
                    self.topic_partition.journal_partition_dir(),
                    new_base_offset
                ),
                self.index_file_max_size as usize,
                false,
            )
            .await?,
            None,
        ));
        segments.insert(new_base_offset, new_seg);
        trace!("日志段滚动到新的基准偏移量: {}", new_base_offset);
        Ok(())
    }

    /// 为内存记录分配偏移量。
    ///
    /// # 参数
    ///
    /// * `topic_partition` - 主题分区。
    /// * `memory_records` - 要分配偏移量的内存记录。
    ///
    /// # 返回
    ///
    /// 返回 `AppResult<()>` 表示成功或失败。
    async fn assign_offset(
        &self,
        topic_partition: &TopicPartition,
        memory_records: &mut MemoryRecords,
    ) -> AppResult<()> {
        let queue_log_next_offset = self
            .queue_next_offset_info
            .entry(topic_partition.clone())
            .or_insert(0);
        let offset = *queue_log_next_offset;
        memory_records.set_base_offset(offset)?;
        Ok(())
    }

    /// 将记录追加到活动段。
    ///
    /// # 参数
    ///
    /// * `topic_partition` - 主题分区。
    /// * `memory_records` - 要追加的内存记录。
    ///
    /// # 返回
    ///
    /// 返回 `AppResult<()>` 表示成功或失败。
    async fn append_to_active_segment(
        &self,
        topic_partition: TopicPartition,
        memory_records: MemoryRecords,
    ) -> AppResult<()> {
        let segments = self.segments.read().await; // 获取读锁以进行并发读取
        let active_seg = segments
            .values()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error())?;

        let (tx, rx) = oneshot::channel();
        active_seg
            .append_record((self.next_offset.load(), topic_partition, memory_records, tx))
            .await?;
        rx.await??;
        Ok(())
    }

    /// 更新偏移量信息。
    ///
    /// # 参数
    ///
    /// * `record_count` - 追加的记录数。
    async fn update_offsets(&self, record_count: u32) {
        let mut queue_next_offset = self
            .queue_next_offset_info
            .entry(self.topic_partition.clone())
            .or_insert(0);
        *queue_next_offset += record_count as i64;

        self.next_offset.fetch_add(record_count as u64);
    }

    /// 确定下一个段的基准偏移量。
    ///
    /// # 参数
    ///
    /// * `current_base_offset` - 当前段的基准偏移量。
    ///
    /// # 返回
    ///
    /// 返回下一个段的基准偏移量。如果没有下一个段，则返回日志的下一个偏移量。
    async fn next_segment_base_offset(&self, current_base_offset: u64) -> u64 {
        let segments = self.segments.read().await;
        segments
            .range((current_base_offset + 1)..)
            .next()
            .map(|(&base_offset, _)| base_offset)
            .unwrap_or_else(|| self.next_offset.load())
    }

    /// 生成未找到活动段的错误。
    ///
    /// # 返回
    ///
    /// 返回一个 `AppError` 表示未找到活动段。
    fn no_active_segment_error(&self) -> AppError {
        CommonError(format!("未找到活动段，主题分区: {}", self.topic_partition))
    }

    /// 从已有的数据加载 `JournalLog`。
    ///
    /// # 参数
    ///
    /// * `topic_partition` - 主题分区。
    /// * `recover_point` - 恢复点偏移量。
    /// * `split_offset` - 分割偏移量。
    /// * `dir` - 数据目录。
    /// * `max_index_file_size` - 最大索引文件大小。
    /// * `rt` - Tokio 运行时。
    ///
    /// # 返回
    ///
    /// 返回加载的 `JournalLog` 实例。
    pub fn load_from(
        topic_partition: &TopicPartition,
        recover_point: u64,
        split_offset: u64,
        dir: impl AsRef<Path>,
        index_file_max_size: u32,

        rt: &Runtime,
    ) -> AppResult<Self> {
        let segments = Self::load_segments(topic_partition, dir, index_file_max_size as usize, rt)?;

        let queue_next_offset_checkpoint_path = format!(
            "{}/{}",
            &topic_partition.journal_partition_dir(),
            NEXT_OFFSET_CHECKPOINT_FILE_NAME
        );
        let queue_next_offset_checkpoints = CheckPointFile::new(queue_next_offset_checkpoint_path);

        let queue_next_offset = rt.block_on(queue_next_offset_checkpoints.read_checkpoints())?;

        let log_start_offset = segments
            .first_key_value()
            .map(|(offset, _)| *offset)
            .unwrap_or(0);
        let next_offset = recover_point + 1;

        let log = JournalLog {
            segments: RwLock::new(segments),
            queue_next_offset_info: DashMap::from_iter(
                queue_next_offset.into_iter().map(|(k, v)| (k, v as i64)),
            ),
            queue_next_offset_checkpoints,
            log_start_offset: AtomicCell::new(log_start_offset),
            next_offset: AtomicCell::new(next_offset),
            recover_point: AtomicCell::new(recover_point),
            split_offset: AtomicCell::new(split_offset),
            write_lock: RwLock::new(()),
            topic_partition: topic_partition.clone(),
            index_file_max_size,
        };

        Ok(log)
    }

    /// 加载指定目录下的所有日志段文件。
    ///
    /// # 参数
    ///
    /// * `topic_partition` - 主题分区。
    /// * `dir` - 数据目录。
    /// * `max_index_file_size` - 最大索引文件大小。
    /// * `rt` - Tokio 运行时。
    ///
    /// # 返回
    ///
    /// 返回加载的日志段映射。
    fn load_segments(
        topic_partition: &TopicPartition,
        dir: impl AsRef<Path>,
        max_index_file_size: usize,
        rt: &Runtime,
    ) -> AppResult<BTreeMap<u64, Arc<LogSegment>>> {
        let mut index_files = BTreeMap::new();
        let mut log_files = BTreeMap::new();

        let mut read_dir = std::fs::read_dir(dir)?;
        while let Some(file) = read_dir.next().transpose()? {
            if file.metadata()?.file_type().is_file() {
                let file_name = file.file_name().to_string_lossy().to_string();
                let dot = file_name.rfind('.');
                match dot {
                    None => {
                        warn!("无效的段文件名: {}", file_name);
                        continue;
                    }
                    Some(dot) => {
                        let file_prefix = &file_name[..dot];
                        let file_suffix = &file_name[dot..];
                        match file_suffix {
                            ".timeindex" => {
                                // journal log 不应有时间索引文件
                                continue;
                            }
                            ".index" => {
                                // journal log 不应有偏移索引文件
                                let index_file = rt.block_on(IndexFile::new(
                                    file.path(),
                                    max_index_file_size,
                                    false,
                                ))?;
                                index_files.insert(file_prefix.parse::<u64>().unwrap(), index_file);
                            }
                            ".log" => {
                                let base_offset = file_prefix.parse::<u64>();
                                match base_offset {
                                    Ok(base_offset) => {
                                        let file_records =
                                            rt.block_on(FileRecords::open(file.path()))?;
                                        log_files.insert(base_offset, file_records);
                                    }
                                    Err(_) => {
                                        warn!("无效的段文件名: {}", file_prefix);
                                        continue;
                                    }
                                }
                            }
                            other => {
                                warn!("无效的段文件名: {}", other);
                                continue;
                            }
                        }
                    }
                }
            }
        }
        let mut segments = BTreeMap::new();
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
                segments.insert(base_offset, Arc::new(segment));
            } else {
                error!("未找到偏移索引文件: {}", base_offset);
            }
        }

        Ok(segments)
    }
}
