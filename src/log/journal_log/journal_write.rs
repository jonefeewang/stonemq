use tokio::sync::oneshot;
use tracing::{debug, trace};

use crate::{
    global_config,
    log::{log_segment::LogSegment, IndexFile, LogAppendInfo, LogType, DEFAULT_LOG_APPEND_TIME},
    message::{MemoryRecords, RecordBatch, TopicPartition},
    AppError, AppResult,
};

use super::JournalLog;

impl JournalLog {
    /// 追加记录到日志。
    ///
    /// # 参数
    ///
    /// * `records` - 包含主题分区和要追加的内存记录的元组。
    ///
    /// # 返回
    ///
    /// 返回包含追加操作信息的 `AppResult<LogAppendInfo>`。
    pub async fn append_records(
        &self,
        records: (TopicPartition, MemoryRecords),
    ) -> AppResult<LogAppendInfo> {
        let (queue_topic_partition, mut memory_records) = records;

        // 分解为record batch
        let mut record_batches = vec![];
        for batch in &mut memory_records {
            record_batches.push(batch);
        }

        let _write_guard = self.write_lock.lock().await; // 获取写锁以进行原子追加操作

        let log_append_info = self.validate_records_and_assign_queue_offset(
            &queue_topic_partition,
            record_batches,
            &mut memory_records,
        )?;

        {
            let dashmap_value = self
                .queue_next_offset_info
                .get(&queue_topic_partition)
                .unwrap();
            trace!(
                "topic partition: {} append_records 结束offset: {},dashmap中的topic partition entry value: {:?}",
                queue_topic_partition.id(),
                self.next_offset.load(),
                *dashmap_value
            );
        }

        let (active_seg_size, active_segment_offset_index_full) =
            self.get_active_segment_info().await?;

        if self
            .need_roll(
                active_seg_size,
                &memory_records,
                active_segment_offset_index_full,
            )
            .await
        {
            self.roll_segment().await?;
        }

        self.append_to_active_segment(
            queue_topic_partition.clone(),
            log_append_info.first_offset,
            log_append_info.last_offset,
            log_append_info.records_count,
            memory_records,
        )
        .await?;

        // 追加一个批次，偏移量加1
        self.next_offset.fetch_add(1);

        // self.update_offsets(queue_topic_partition.clone(), log_append_info.records_count)
        //     .await;

        drop(_write_guard);

        Ok(log_append_info)
    }

    fn validate_records_and_assign_queue_offset(
        &self,
        queue_topic_partition: &TopicPartition,
        record_batches: Vec<RecordBatch>,
        memory_records: &mut MemoryRecords,
    ) -> AppResult<LogAppendInfo> {
        let mut max_timestamp = -1i64;
        let mut offset_of_max_timestamp = -1i64;
        let mut first_offset = -1i64;
        let mut last_offset = -1i64;
        let mut records_count: u32 = 0;
        let mut queue_next_offset = self
            .queue_next_offset_info
            .entry(queue_topic_partition.clone())
            .or_insert(0);

        trace!(
            "validate_records_and_assign_offset 开始offset: {}, topic partition: {}",
            *queue_next_offset,
            queue_topic_partition.id()
        );

        if first_offset == -1 {
            first_offset = *queue_next_offset;
        }
        for mut batch in record_batches {
            let _ = &batch.validate_batch()?;
            let records = batch.records();
            for record in records {
                // todo: validate records
                records_count += 1;
                *queue_next_offset += 1;
                let record_time_stamp = batch.base_timestamp() + record.timestamp_delta;
                if record_time_stamp > max_timestamp {
                    max_timestamp = record_time_stamp;
                    offset_of_max_timestamp = *queue_next_offset;
                }
            }

            batch.set_base_offset(*queue_next_offset - batch.last_offset_delta() as i64 - 1);
            last_offset = batch.base_offset();
            // batch.set_first_timestamp(max_timestamp);
            trace!(
                "batch xxx topic partition: {}, base_offset: {}, queue_next_offset: {}",
                queue_topic_partition.id(),
                batch.base_offset(),
                *queue_next_offset
            );
            batch.unsplit(memory_records);
        }

        trace!(
            "validate_records_and_assign_offset 结束offset:{:?}",
            *queue_next_offset,
        );

        Ok(LogAppendInfo {
            first_offset,
            last_offset,
            _max_timestamp: max_timestamp,
            _offset_of_max_timestamp: offset_of_max_timestamp,
            records_count,
            _log_append_time: DEFAULT_LOG_APPEND_TIME,
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
    pub async fn flush(&self) -> AppResult<()> {
        let segments = self.segments.read().await; // 获取读锁以进行并发读取
        let active_seg = segments
            .values()
            .next_back()
            .ok_or_else(|| self.no_active_segment_error())?;
        self.flush_segment(active_seg).await?;
        Ok(())
    }

    async fn flush_segment(&self, active_seg: &LogSegment) -> AppResult<()> {
        active_seg.flush().await?;
        self.recover_point.store(self.next_offset.load() - 1);
        debug!(
            "topic partition: {} flush_segment 结束offset: {},recover_point: {}",
            self.topic_partition.id(),
            self.next_offset.load(),
            self.recover_point.load()
        );
        Ok(())
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
        active_seg_size: u32,
        memory_records: &MemoryRecords,
        active_segment_offset_index_full: bool,
    ) -> bool {
        let config = &global_config().log;
        let total_size = Self::calculate_journal_log_overhead(&self.topic_partition)
            + memory_records.size() as u32
            + active_seg_size;

        let condition =
            total_size >= config.journal_segment_size as u32 || active_segment_offset_index_full;
        if condition {
            debug!(
                "roll segment total_size: {},config size: {}, active_seg_index_full: {}",
                total_size, config.journal_segment_size, active_segment_offset_index_full
            );
        }
        condition
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

        self.flush_segment(active_seg).await?;

        let new_base_offset = self.next_offset.load();

        let index_file = IndexFile::new(
            format!(
                "{}/{}.index",
                self.topic_partition.journal_partition_dir(),
                new_base_offset
            ),
            self.index_file_max_size as usize,
            false,
        )
        .await
        .map_err(|e| AppError::DetailedIoError(format!("open index file error: {}", e)))?;

        let mut new_seg = LogSegment::open(
            self.topic_partition.clone(),
            new_base_offset,
            index_file,
            None,
        );
        new_seg.open_file_records(&self.topic_partition).await?;

        segments.insert(new_base_offset, new_seg);
        debug!("journal log roll segment: {}", new_base_offset);
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
        queue_topic_partition: TopicPartition,
        first_offset: i64,
        last_offset: i64,
        records_count: u32,
        memory_records: MemoryRecords,
    ) -> AppResult<()> {
        let segments = self.segments.read().await; // 获取读锁以进行并发读取
        let active_seg = segments.values().next_back().ok_or_else(|| {
            AppError::IllegalStateError(format!(
                "active segment not found for topic partition: {}",
                self.topic_partition.id()
            ))
        })?;

        let (tx, rx) = oneshot::channel();
        active_seg
            .append_record(
                LogType::Journal,
                (
                    self.next_offset.load(),
                    queue_topic_partition,
                    first_offset,
                    last_offset,
                    records_count,
                    memory_records,
                    tx,
                ),
            )
            .await?;
        rx.await
            .map_err(|e| AppError::ChannelRecvError(e.to_string()))??;
        Ok(())
    }

    /// 生成未找到活动段的错误。
    ///
    /// # 返回
    ///
    /// 返回一个 `AppError` 表示未找到活动段。
    pub fn no_active_segment_error(&self) -> AppError {
        AppError::IllegalStateError(format!(
            "no active segment, topic partition: {}",
            self.topic_partition.id()
        ))
    }

    pub async fn close(&self) -> AppResult<()> {
        self.flush().await?;

        Ok(())
    }
}
