mod journal_load;
mod journal_read;
mod journal_write;

use std::collections::BTreeMap;

use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
use tokio::sync::{Mutex, RwLock};

use crate::{
    global_config,
    log::{CheckPointFile, NEXT_OFFSET_CHECKPOINT_FILE_NAME},
    message::TopicPartition,
    AppError, AppResult,
};

use super::log_segment::LogSegment;

/// 表示日志分区的日志管理器。
///
/// 负责管理日志段、追加记录以及滚动日志段。
#[derive(Debug)]
pub struct JournalLog {
    /// 日志段的有序映射，使用 `RwLock` 以允许多个并发读取和单一写入。
    /// 写入： roll的时候才会用到
    /// 读取： 很多地方会用到,相对于读取的频率，write的频率确实不高
    segments: RwLock<BTreeMap<i64, LogSegment>>,

    /// 队列下一个偏移信息，使用 `DashMap` 以提供并发安全的哈希映射。
    queue_next_offset_info: DashMap<TopicPartition, i64>,

    /// 日志开始偏移量。
    _log_start_offset: AtomicCell<i64>,

    /// 下一个偏移量。
    next_offset: AtomicCell<i64>,

    /// 恢复点偏移量。
    pub(crate) recover_point: AtomicCell<i64>,

    /// 分割偏移量。
    pub split_offset: AtomicCell<i64>,

    /// 写操作的锁。
    /// 控制写入文件/更新segments/更新offset等系列复合操作，无法使用channel，只能使用锁
    write_lock: Mutex<()>,

    /// 主题分区信息。
    topic_partition: TopicPartition,

    /// 最大索引文件大小。
    index_file_max_size: u32,

    queue_next_offset_checkpoints: CheckPointFile,
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
    pub async fn new(topic_partition: &TopicPartition) -> AppResult<Self> {
        let dir = topic_partition.journal_partition_dir();
        tokio::fs::create_dir_all(&dir).await.map_err(|e| {
            AppError::DetailedIoError(format!(
                "create journal partition dir: {} error: {}",
                dir, e
            ))
        })?;

        let index_file_max_size = global_config().log.journal_index_file_size;
        let segment = LogSegment::new(topic_partition, dir, 0, index_file_max_size as u32).await?;

        let mut segments = BTreeMap::new();
        segments.insert(0, segment);

        Ok(Self {
            segments: RwLock::new(segments),
            queue_next_offset_info: DashMap::new(),
            queue_next_offset_checkpoints: CheckPointFile::new(format!(
                "{}/{}",
                &topic_partition.journal_partition_dir(),
                NEXT_OFFSET_CHECKPOINT_FILE_NAME
            )),
            _log_start_offset: AtomicCell::new(0),
            next_offset: AtomicCell::new(0),
            recover_point: AtomicCell::new(-1),
            split_offset: AtomicCell::new(-1),
            write_lock: Mutex::new(()),
            topic_partition: topic_partition.clone(),
            index_file_max_size: index_file_max_size as u32,
        })
    }
}
