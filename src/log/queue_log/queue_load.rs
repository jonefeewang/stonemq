use crate::log::IndexFile;
use crate::log::{file_records::FileRecords, log_segment::LogSegment};
use crate::message::TopicPartition;
use crate::{AppError, AppResult};
use crossbeam::atomic::AtomicCell;
use std::collections::BTreeMap;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use super::QueueLog;

impl QueueLog {
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

        if std::fs::read_dir(&dir)
            .map_err(|e| {
                AppError::DetailedIoError(format!(
                    "read dir: {} error: {} while loading queue log",
                    dir, e
                ))
            })?
            .next()
            .is_none()
        {
            info!("队列日志目录为空：{}", &dir);
            return Ok(segments);
        }

        let mut index_files = BTreeMap::new();
        let mut log_files = BTreeMap::new();

        let mut read_dir = std::fs::read_dir(&dir).map_err(|e| {
            AppError::DetailedIoError(format!(
                "read dir: {} error: {} while loading queue log",
                dir, e
            ))
        })?;
        while let Some(file) = read_dir.next().transpose().map_err(|e| {
            AppError::DetailedIoError(format!(
                "read dir: {} error: {} while loading queue log",
                dir, e
            ))
        })? {
            if file
                .metadata()
                .map_err(|e| {
                    AppError::DetailedIoError(format!(
                        "get file metadata: {} error: {} while loading queue log",
                        file.path().to_string_lossy(),
                        e
                    ))
                })?
                .file_type()
                .is_file()
            {
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
                                let index_file = rt
                                    .block_on(IndexFile::new(
                                        file.path(),
                                        index_file_max_size as usize,
                                        false,
                                    ))
                                    .map_err(|e| {
                                        AppError::DetailedIoError(format!("open index file error: {}", e))
                                    })?;
                                index_files.insert(
                                    file_prefix.parse::<i64>().map_err(|_| {
                                        AppError::InvalidValue(format!(
                                            "invalid index file name: {}",
                                            file_prefix
                                        ))
                                    })?,
                                    index_file,
                                );
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
}
