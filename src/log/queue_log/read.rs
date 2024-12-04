use crate::global_config;
use crate::log::{file_records::FileRecords, log_segment::PositionInfo};
use crate::log::{LogType, NO_POSITION_INFO};
use crate::message::{LogFetchInfo, MemoryRecords, TopicPartition};
use crate::AppResult;
use bytes::BytesMut;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tracing::{debug, trace};

use super::QueueLog;

impl QueueLog {
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

    /// 返回包含位置信息的 `AppResult<PositionInfo>`。
    pub async fn get_reference_position_info(&self, offset: i64) -> AppResult<PositionInfo> {
        let segments = self.segments.read().await; // 获取读锁以进行并发读取
        let segment = segments
            .range(..=offset)
            .next_back()
            .map(|(_, segment)| segment)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("未找到偏移量 {} 的段", offset),
                )
            })?;
        segment.get_relative_position(offset).await
    }

    pub async fn calc_max_read_size(&self, base_offset: i64) -> Option<usize> {
        let segments = self.segments.read().await;
        let floor_segment = segments.range(..=base_offset).next_back();
        if floor_segment.is_some() {
            // active segment
            let (_, active_segment) = floor_segment.unwrap();
            Some(active_segment.size())
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
        let ref_position_info = self.get_reference_position_info(start_offset).await?;
        trace!("ref_position_info: {:?}", ref_position_info);
        let queue_topic_dir =
            PathBuf::from(global_config().log.queue_base_dir.clone()).join(topic_partition.id());
        let segment_path = queue_topic_dir.join(format!("{}.log", ref_position_info.base_offset));
        let queue_seg_file = fs::File::open(&segment_path).await?;

        // 这里会报UnexpectedEof，其他io错误,以及not found，总之无法继续读取消息了，下游需要重试
        let seek_result = FileRecords::seek(
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

        let total_len = segment_file.metadata().await?.len();

        // 如果是非活动段的话，直接取到末尾，就是一个record完整结束，如果是活动段的话，可能会截断
        let max_position = if let Some(max_read_size) =
            self.calc_max_read_size(ref_position_info.base_offset).await
        {
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
            let mut buffer = BytesMut::zeroed(left_len as usize);
            let _ = segment_file.read(&mut buffer).await?;

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
        let mut buffer = BytesMut::zeroed(max_size as usize);
        let _ = segment_file.read(&mut buffer).await?;
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
