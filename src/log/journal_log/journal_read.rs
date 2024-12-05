use tracing::error;

use crate::{log::PositionInfo, message::TopicPartition, AppError, AppResult};

use super::JournalLog;

impl JournalLog {
    /// 获取给定偏移量的位置信息。
    ///
    /// # 参数
    ///
    /// * `offset` - 要获取位置信息的偏移量。
    ///
    /// # 返回
    ///
    /// 返回包含位置信息的 `AppResult<PositionInfo>`。
    pub async fn get_relative_position_info(&self, offset: i64) -> AppResult<PositionInfo> {
        let segments = self.segments.read().await; // 获取读锁以进行并发读取
        let segment = segments
            .range(..=offset)
            .next_back()
            .map(|(_, segment)| segment)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("not found segment for offset: {}", offset),
                )
            })?;
        segment.get_relative_position(offset).await
    }

    /// 获取当前活动段的偏移量。
    ///
    /// # 返回
    ///
    /// 返回包含活动段偏移量的 `AppResult<u64>`。
    pub async fn current_active_seg_offset(&self) -> AppResult<i64> {
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
    pub async fn get_active_segment_info(&self) -> AppResult<(u32, bool)> {
        let segments = self.segments.read().await; // 获取读锁以进行并发读取
        let active_seg = segments.values().next_back().ok_or_else(|| {
            error!("未找到活动段，主题分区: {}", self.topic_partition.id());
            AppError::IllegalStateError(format!(
                "未找到活动段，主题分区: {}",
                self.topic_partition.id()
            ))
        })?;
        Ok((
            active_seg.size() as u32,
            active_seg.offset_index_full().await?,
        ))
    }
    /// Calculates the overhead for a journal log record.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition.
    ///
    /// # Returns
    ///
    /// Returns the calculated overhead as a u32.
    pub fn calculate_journal_log_overhead(topic_partition: &TopicPartition) -> u32 {
        //offset + tpstr size + tpstr + i64(first_batch_queue_base_offset)+i64(last_batch_queue_base_offset)+u32(records_count)
        8 + topic_partition.protocol_size() + 8 + 8 + 4
    }
}
