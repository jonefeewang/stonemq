use std::{collections::BTreeMap, time::Duration};

use tokio::time::Instant;


use crate::{

    AppError, AppResult,
};
use crate::request::fetch::{FetchRequest, FetchResponse};
use super::ReplicaManager;

impl ReplicaManager {
    pub async fn fetch(&self, request: &FetchRequest) -> AppResult<FetchResponse> {
        let max_wait = Duration::from_millis(request.max_wait as u64);
        let start_time = Instant::now();

        loop {
            match self.try_fetch(request).await {
                Ok(response) => return Ok(response),
                Err(_) if start_time.elapsed() >= max_wait => {
                    // 时间耗尽,返回当前读取结果
                    return Ok(FetchResponse::from(self.try_fetch(request).await?));
                }
                Err(_) => {
                    // 等待一小段时间后重试
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    async fn try_fetch(&self, request: &FetchRequest) -> AppResult<FetchResponse> {
        let mut limit = request.max_bytes;
        let mut read_result = BTreeMap::new();

        for (topic_partition, partition_data) in &request.fetch_data {
            let queue_partition = self.all_queue_partitions.get(topic_partition).unwrap();
            let (records, log_start_offset, log_end_offset) = queue_partition
                .read_records(partition_data.fetch_offset, limit)
                .await?;
            limit = (limit - records.size() as i32).max(0);
            read_result.insert(
                topic_partition.clone(),
                (records, log_start_offset, log_end_offset),
            );
        }

        let total_size = read_result
            .iter()
            .map(|(_, (records, _, _))| records.size() as i32)
            .sum::<i32>();

        if total_size > request.min_bytes {
            Ok(FetchResponse::from(read_result))
        } else {
            Err(AppError::InsufficientData)
        }
    }
}
