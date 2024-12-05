use std::{collections::BTreeMap, path::Path, sync::Arc};

use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
use tokio::{
    runtime::Runtime,
    sync::{Mutex, RwLock},
};
use tracing::{error, info, trace, warn};

use crate::{
    log::{
        file_records::FileRecords, log_segment::LogSegment, CheckPointFile, IndexFile,
        NEXT_OFFSET_CHECKPOINT_FILE_NAME,
    },
    message::TopicPartition,
    AppError, AppResult,
};

use super::JournalLog;
impl JournalLog {
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
        recover_point: i64,
        split_offset: i64,
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
        trace!(
            "load journal log queue_next_offset: {:?}",
            queue_next_offset
        );

        let log_start_offset = segments
            .first_key_value()
            .map(|(offset, _)| *offset)
            .unwrap_or(0);
        let next_offset = recover_point + 1;

        let log = JournalLog {
            segments: RwLock::new(segments),
            queue_next_offset_info: DashMap::from_iter(queue_next_offset),
            queue_next_offset_checkpoints,
            _log_start_offset: AtomicCell::new(log_start_offset),
            next_offset: AtomicCell::new(next_offset),
            recover_point: AtomicCell::new(recover_point),
            split_offset: AtomicCell::new(split_offset),
            write_lock: Mutex::new(()),
            topic_partition: topic_partition.clone(),
            index_file_max_size,
        };

        info!(
            "load journal log:{} next_offset:{},recover_point:{},split_offset:{}",
            topic_partition.id(),
            log.next_offset.load(),
            log.recover_point.load(),
            log.split_offset.load()
        );

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
    ) -> AppResult<BTreeMap<i64, Arc<LogSegment>>> {
        let mut index_files = BTreeMap::new();
        let mut log_files = BTreeMap::new();

        let mut read_dir = std::fs::read_dir(&dir).map_err(|e| {
            AppError::DetailedIoError(format!(
                "read dir: {} error: {} while loading journal log",
                &dir.as_ref().to_string_lossy(),
                e
            ))
        })?;
        while let Some(file) = read_dir.next().transpose().map_err(|e| {
            AppError::DetailedIoError(format!(
                "read dir: {} error: {} while loading journal log",
                dir.as_ref().to_string_lossy(),
                e
            ))
        })? {
            if file
                .metadata()
                .map_err(|e| {
                    AppError::DetailedIoError(format!(
                        "get file metadata: {} error: {} while loading journal log",
                        file.path().to_string_lossy(),
                        e
                    ))
                })?
                .file_type()
                .is_file()
            {
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
                                let index_file = rt
                                    .block_on(IndexFile::new(
                                        file.path(),
                                        max_index_file_size,
                                        false,
                                    ))
                                    .map_err(|e| {
                                        AppError::DetailedIoError(format!("open index file error: {}", e))
                                    })?;
                                index_files.insert(file_prefix.parse::<i64>().unwrap(), index_file);
                            }
                            ".log" => {
                                let base_offset = file_prefix.parse::<i64>();
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

    pub async fn checkpoint_next_offset(&self) -> AppResult<()> {
        let queue_next_offset_info = self
            .queue_next_offset_info
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();
        self.queue_next_offset_checkpoints
            .write_checkpoints(queue_next_offset_info)
            .await
            .map_err(|e| AppError::DetailedIoError(format!("write checkpoint error: {}", e)))
    }
}
