use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use bytes::BytesMut;
use tracing::{debug, trace};

use crate::{
    log::{log_segment::LogSegmentCommon, seek, LogType, PositionInfo, NO_POSITION_INFO},
    message::{LogFetchInfo, MemoryRecords, TopicPartition},
    AppError, AppResult,
};

use super::QueueLog;

/// read config
#[derive(Debug, Clone)]
struct ReadConfig {
    max_position: u64,
    target_offset_position: u64,
    max_size: i32,
}

impl QueueLog {
    /// get recover point offset
    pub fn get_recover_point(&self) -> i64 {
        self.recover_point.load()
    }

    /// get LEO(Log End Offset) info
    pub fn get_leo_info(&self) -> AppResult<PositionInfo> {
        Ok(PositionInfo {
            base_offset: self.active_segment_id.load(),
            offset: self.last_offset.load(),
            position: self.active_segment.read().size() as i64,
        })
    }

    /// get position info for offset
    pub fn get_reference_position_info(&self, offset: i64) -> AppResult<PositionInfo> {
        let segment_offset = self.find_segment_for_offset(offset)?;
        self.get_segment_position(segment_offset, offset)
    }

    /// find segment for offset
    fn find_segment_for_offset(&self, offset: i64) -> AppResult<i64> {
        self.segments_order
            .read()
            .iter()
            .rev()
            .find(|&&seg_offset| seg_offset <= offset)
            .copied()
            .ok_or_else(|| {
                AppError::InvalidValue(format!("no segment found for offset {}", offset))
            })
    }

    /// get segment position info
    fn get_segment_position(&self, segment_offset: i64, offset: i64) -> AppResult<PositionInfo> {
        if segment_offset == self.active_segment_id.load() {
            self.active_segment.read().get_relative_position(offset)
        } else {
            self.segments
                .get(&segment_offset)
                .ok_or_else(|| {
                    AppError::InvalidValue(format!("segment not found for offset {}", offset))
                })?
                .get_relative_position(offset)
        }
    }

    /// read records
    pub async fn read_records(
        &self,
        topic_partition: &TopicPartition,
        start_offset: i64,
        max_size: i32,
    ) -> AppResult<LogFetchInfo> {
        trace!(
            "Reading records from partition: {}, start_offset: {}, max_size: {}",
            topic_partition.id(),
            start_offset,
            max_size
        );

        // retrieve the segment information where the `start_offset` resides.
        let ref_position_info = match self.get_reference_position_info(start_offset) {
            Ok(info) => info,
            Err(_) => return self.create_empty_fetch_info(),
        };

        // open the segment file where the `start_offset` resides.
        let segment_file = self.open_segment_file(topic_partition, &ref_position_info)?;

        // seek file to the target position
        let (file, target_position_info) = match self
            .seek_to_position(segment_file, start_offset, ref_position_info)
            .await
        {
            Ok(result) => result,
            Err(_) => return self.create_empty_fetch_info(),
        };

        // calculate read config
        let read_config = self.calculate_read_config(
            &ref_position_info,
            target_position_info.position as u64,
            max_size,
        )?;

        // do read records
        self.do_read_records(file, topic_partition, read_config, target_position_info)
            .await
    }

    /// open segment file
    fn open_segment_file(
        &self,
        topic_partition: &TopicPartition,
        position_info: &PositionInfo,
    ) -> AppResult<File> {
        let segment_path = PathBuf::from(topic_partition.partition_dir())
            .join(format!("{}.log", position_info.base_offset));

        File::open(&segment_path).map_err(|e| {
            AppError::DetailedIoError(format!(
                "Failed to open segment file: {} error: {}",
                segment_path.display(),
                e
            ))
        })
    }

    /// seek to position
    async fn seek_to_position(
        &self,
        file: File,
        start_offset: i64,
        position_info: PositionInfo,
    ) -> AppResult<(File, PositionInfo)> {
        seek(file, start_offset, position_info, LogType::Queue)
            .await
            .map_err(|e| AppError::DetailedIoError(format!("Failed to seek to position: {}", e)))
    }

    /// calculate read config
    fn calculate_read_config(
        &self,
        position_info: &PositionInfo,
        target_position: u64,
        max_size: i32,
    ) -> AppResult<ReadConfig> {
        let max_position = self.calc_max_read_size(position_info.base_offset) as u64;

        Ok(ReadConfig {
            max_position,
            target_offset_position: target_position,
            max_size,
        })
    }

    /// calculate max read size
    /// active segment can read to size, inactive segment can read to segment_size
    fn calc_max_read_size(&self, base_offset: i64) -> usize {
        let segments = self.segments_order.read();
        segments
            .range(..=base_offset)
            .next_back()
            .copied()
            .map(|offset| {
                if offset == self.active_segment_id.load() {
                    self.active_segment.read().size() as usize
                } else {
                    self.segments.get(&offset).unwrap().size() as usize
                }
            })
            .unwrap()
    }

    /// do read records
    async fn do_read_records(
        &self,
        file: File,
        topic_partition: &TopicPartition,
        config: ReadConfig,
        target_position_info: PositionInfo,
    ) -> AppResult<LogFetchInfo> {
        let ReadConfig {
            max_position,
            target_offset_position,
            max_size,
        } = config;

        let left_len = max_position - target_offset_position;

        debug!(
            "Reading from partition: {}, remaining length: {}",
            topic_partition.id(),
            left_len
        );

        if left_len == 0 {
            return self.create_empty_fetch_info();
        }

        let read_size = if left_len < max_size as u64 {
            left_len as usize
        } else {
            max_size as usize
        };

        let buffer = self.read_file_chunk(file, read_size).await?;
        let records = MemoryRecords::new(buffer);

        trace!(
            "First batch base offset: {:?}",
            records.first_batch_base_offset()
        );

        Ok(LogFetchInfo {
            records,
            log_start_offset: self.log_start_offset,
            log_end_offset: self.last_offset.load(),
            position_info: target_position_info,
        })
    }

    /// read file chunk
    async fn read_file_chunk(&self, mut file: File, size: usize) -> AppResult<BytesMut> {
        tokio::task::spawn_blocking(move || {
            let mut buffer = BytesMut::zeroed(size);
            file.read_exact(&mut buffer)?;
            Ok(buffer)
        })
        .await
        .map_err(|e| AppError::DetailedIoError(format!("Failed to read file: {}", e)))?
    }

    /// create empty fetch info
    fn create_empty_fetch_info(&self) -> AppResult<LogFetchInfo> {
        Ok(LogFetchInfo {
            records: MemoryRecords::empty(),
            log_start_offset: self.log_start_offset,
            log_end_offset: self.last_offset.load(),
            position_info: NO_POSITION_INFO,
        })
    }
}
