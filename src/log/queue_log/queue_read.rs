use crate::log::log_segment::LogSegmentCommon;
use crate::log::{log_segment::PositionInfo, LogType, NO_POSITION_INFO};
use crate::message::{LogFetchInfo, MemoryRecords, TopicPartition};
use crate::AppResult;
use crate::{global_config, AppError};
use bytes::BytesMut;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use tracing::{debug, trace};

use super::QueueLog;

impl QueueLog {
    pub fn get_recover_point(&self) -> i64 {
        self.recover_point.load()
    }

    pub fn get_leo_info(&self) -> AppResult<PositionInfo> {
        let leo_info = PositionInfo {
            base_offset: self.active_segment_id.load(),
            offset: self.last_offset.load(),
            position: self.active_segment.read().size() as i64,
        };
        Ok(leo_info)
    }

    /// 返回包含位置信息的 `AppResult<PositionInfo>`。
    pub fn get_reference_position_info(&self, offset: i64) -> AppResult<PositionInfo> {
        // Find the segment containing this offset by looking for the largest base offset
        // that is less than or equal to the target offset
        let segment_offset = self
            .segments_order
            .read()
            .iter()
            .rev()
            .find(|&&seg_offset| seg_offset <= offset)
            .copied() // Convert reference to value
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("no segment found for offset {}", offset),
                )
            })?;

        // Check if the offset belongs to the active segment
        if segment_offset == self.active_segment_id.load() {
            self.active_segment.read().get_relative_position(offset)
        } else {
            // Look up in the inactive segments
            self.segments
                .get(&segment_offset)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("segment not found for offset {}", offset),
                    )
                })?
                .get_relative_position(offset)
        }
    }

    pub fn calc_max_read_size(&self, base_offset: i64) -> Option<usize> {
        let floor_segment = self
            .segments_order
            .read()
            .range(..=base_offset)
            .next_back()
            .copied();
        if let Some(base_offset) = floor_segment {
            if base_offset == self.active_segment_id.load() {
                return Some(self.active_segment.read().size() as usize);
            } else {
                let ref_segment = self.segments.get(&base_offset).unwrap().size();
                return Some(ref_segment as usize);
            }
        } else {
            None
        }
    }

    pub async fn read_records(
        &self,
        topic_partition: &TopicPartition,
        start_offset: i64,
        max_size: i32,
    ) -> AppResult<LogFetchInfo> {
        trace!(
            "queue read_records topic partition: {}, start_offset: {}, max_size: {}",
            topic_partition.id(),
            start_offset,
            max_size
        );
        let ref_position_info = self.get_reference_position_info(start_offset)?;
        trace!("ref_position_info: {:?}", ref_position_info);
        let queue_topic_dir =
            PathBuf::from(global_config().log.queue_base_dir.clone()).join(topic_partition.id());
        let segment_path = queue_topic_dir.join(format!("{}.log", ref_position_info.base_offset));
        let queue_seg_file = File::open(&segment_path).map_err(|e| {
            AppError::DetailedIoError(format!(
                "open queue segment file: {} error: {} while read records",
                segment_path.to_string_lossy(),
                e
            ))
        })?;

        // 这里会报UnexpectedEof，其他io错误,以及not found，总之无法继续读取消息了，下游需要重试
        let seek_result = crate::log::seek(
            queue_seg_file,
            start_offset,
            ref_position_info,
            LogType::Queue,
        )
        .await;

        trace!("seek_result: {:?}", seek_result);

        if seek_result.is_err() {
            return Ok(LogFetchInfo {
                records: MemoryRecords::empty(),
                log_start_offset: self.log_start_offset,
                log_end_offset: self.last_offset.load(),
                position_info: NO_POSITION_INFO,
            });
        }

        let (mut segment_file, current_position) = seek_result.unwrap();

        let total_len = segment_file
            .metadata()
            .map_err(|e| {
                AppError::DetailedIoError(format!(
                    "get file metadata error: {} while read records",
                    e
                ))
            })?
            .len();

        // 如果是非活动段的话，直接取到末尾，就是一个record完整结束，如果是活动段的话，可能会截断
        let max_position =
            if let Some(max_read_size) = self.calc_max_read_size(ref_position_info.base_offset) {
                // 活动段，需要取到最后一个record的结束
                max_read_size as u64
            } else {
                total_len
            };

        let left_len = max_position - current_position.position as u64;

        debug!(
            "queue read_records topic partition: {}, left_len: {}",
            topic_partition.id(),
            left_len
        );
        if left_len == 0 {
            return Ok(LogFetchInfo {
                records: MemoryRecords::empty(),
                log_start_offset: self.log_start_offset,
                log_end_offset: self.last_offset.load(),
                position_info: current_position,
            });
        }

        if left_len < max_size as u64 {
            // 剩余长度小于max_size，则直接读取剩余所有消息,
            // 如果读的恰好是活动段，因为有并发写入，meta信息可能滞后,读取可能偏少，不过没有关系，读取不够的话，下游会重试

            let buffer = tokio::task::spawn_blocking(move || -> std::io::Result<BytesMut> {
                let mut buffer = BytesMut::zeroed(left_len as usize);
                segment_file.read(&mut buffer)?;
                Ok(buffer)
            })
            .await
            .map_err(|e| {
                AppError::DetailedIoError(format!(
                    "read queue segment file: {} error: {} while read records",
                    segment_path.to_string_lossy(),
                    e
                ))
            })??;

            let records = MemoryRecords::new(buffer);
            trace!(
                "get records first batch base offset: {:?}",
                records.first_batch_base_offset()
            );
            return Ok(LogFetchInfo {
                records,
                log_start_offset: self.log_start_offset,
                log_end_offset: self.last_offset.load(),
                position_info: current_position,
            });
        }

        // 这里需要把max_size转换为真实record的最终截断位置，而不是在record的中间

        let buffer = tokio::task::spawn_blocking(move || -> std::io::Result<BytesMut> {
            let mut buffer = BytesMut::zeroed(max_size as usize);
            segment_file.read(&mut buffer)?;
            Ok(buffer)
        })
        .await
        .map_err(|e| {
            AppError::DetailedIoError(format!(
                "read queue segment file: {} error: {} while read records",
                segment_path.to_string_lossy(),
                e
            ))
        })??;

        let records = MemoryRecords::new(buffer);
        trace!(
            "get records first batch base offset enough---: {:?}",
            records.first_batch_base_offset()
        );
        Ok(LogFetchInfo {
            records,
            log_start_offset: self.log_start_offset,
            log_end_offset: self.last_offset.load(),
            position_info: current_position,
        })
    }
}
