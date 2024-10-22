use std::io::SeekFrom;
use std::path::PathBuf;

use super::QueueLog;
use crate::log::file_records::FileRecords;

use crate::log::log_segment::PositionInfo;
use crate::log::LogType;
use crate::message::{MemoryRecords, TopicPartition};
use crate::{global_config, AppResult};
use bytes::BytesMut;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

impl QueueLog {
    async fn next_segment_base_offset(&self, current_base_offset: i64) -> Option<i64> {
        let segments = self.segments.read().await;
        segments
            .range((current_base_offset + 1)..)
            .next()
            .map(|(&base_offset, _)| base_offset)
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
        segment.get_position(offset).await
    }

    pub async fn read_records(
        &self,
        topic_partition: &TopicPartition,
        start_offset: i64,
        max_size: i32,
    ) -> AppResult<(MemoryRecords, i64, i64, PositionInfo)> {
        let mut ref_position_info = self.get_reference_position_info(start_offset).await?;
        let queue_topic_dir =
            PathBuf::from(global_config().log.queue_base_dir.clone()).join(topic_partition.id());
        let segment_path = queue_topic_dir.join(format!("{}.log", ref_position_info.base_offset));
        let queue_seg_file = fs::File::open(&segment_path).await?;

        // 这里会报UnexpectedEof，其他io错误,以及not found，总之无法继续读取消息了，下游需要重试
        let (segment_file, current_position) = FileRecords::seek(
            queue_seg_file,
            start_offset,
            &ref_position_info,
            LogType::Queue,
        )
        .await?;

        // 校准ref_position_info为准确的文件指针位置
        let mut read_position_info = ref_position_info;
        read_position_info.position = current_position as u64;
        read_position_info.offset = start_offset;

        let total_len = segment_file.metadata().await?.len();
        let left_len = total_len - current_position as u64;

        if left_len < max_size as u64 {
            // 剩余长度小于max_size，则直接读取剩余所有消息,
            // 如果读的恰好是活动段，因为有并发写入，meta信息可能滞后,读取可能偏少，不过没有关系，读取不够的话，下游会重试
            let mut buffer = BytesMut::zeroed(left_len as usize);
            let _ = segment_file.read(&mut buffer).await?;

            let records = MemoryRecords::new(buffer);
            return Ok((
                records,
                self.log_start_offset,
                self.last_offset.load(),
                read_position_info,
            ));
        }

        let mut buffer = BytesMut::zeroed(max_size as usize);
        let _ = segment_file.read(&mut buffer).await?;
        let records = MemoryRecords::new(buffer);
        Ok((
            records,
            self.log_start_offset,
            self.last_offset.load(),
            read_position_info,
        ))
    }
}
