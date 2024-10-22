use std::collections::BTreeMap;
use std::sync::Arc;

use super::ReplicaManager;
use crate::message::delayed_fetch::DelayedFetch;
use crate::message::{MemoryRecords, TopicPartition};
use crate::request::fetch::{FetchRequest, FetchResponse};
use crate::{AppError, AppResult};

impl ReplicaManager {
    pub async fn fetch_message(
        self: Arc<ReplicaManager>,
        request: FetchRequest,
    ) -> AppResult<FetchResponse> {
        match self.do_fetch(&request, false).await {
            Ok(response) => return Ok(response),
            Err(e) => match e {
                AppError::InsufficientData => {
                    let (tx, rx) = tokio::sync::oneshot::channel::<
                        BTreeMap<TopicPartition, (MemoryRecords, i64, i64)>,
                    >();
                    let delay_fetch_keys: Vec<String> = request
                        .fetch_data
                        .keys()
                        .map(|topic_partition| topic_partition.to_string())
                        .collect();
                    let delayed_fetch = DelayedFetch {
                        tx,
                        request,
                        replica_manager: self.clone(),
                    };

                    self.delayed_fetch_purgatory
                        .try_complete_else_watch(&Arc::new(delayed_fetch), delay_fetch_keys)
                        .await;
                    let result = rx.await.unwrap();

                    return Ok(FetchResponse::from(result));
                }
                _ => return Err(e),
            },
        }
    }

    async fn do_fetch(&self, request: &FetchRequest, can_delay: bool) -> AppResult<FetchResponse> {
        let mut limit = request.max_bytes;
        let mut read_result = BTreeMap::new();

        for (topic_partition, partition_data) in &request.fetch_data {
            let queue_partition = self.all_queue_partitions.get(topic_partition).unwrap();
            let (records, log_start_offset, log_end_offset, position_info) = queue_partition
                .read_records(partition_data.fetch_offset, limit)
                .await?;
            limit = (limit - records.size() as i32).max(0);
            read_result.insert(
                topic_partition.clone(),
                (records, log_start_offset, log_end_offset, position_info),
            );
        }

        let total_size = read_result
            .iter()
            .map(|(_, (records, _, _))| records.size() as i32)
            .sum::<i32>();

        if total_size > request.min_bytes || !can_delay {
            Ok(FetchResponse::from(read_result))
        } else {
            Err(AppError::InsufficientData)
        }
    }
}
