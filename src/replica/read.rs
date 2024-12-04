use std::collections::BTreeMap;
use std::sync::Arc;

use super::DelayedFetch;
use super::ReplicaManager;
use crate::log::PositionInfo;
use crate::message::{LogFetchInfo, TopicPartition};
use crate::request::{FetchRequest, FetchResponse};

use crate::AppResult;

impl ReplicaManager {
    pub async fn fetch_message(self: Arc<ReplicaManager>, request: FetchRequest) -> FetchResponse {
        let read_result = self.do_fetch(&request).await;
        if read_result.is_err() {
            return FetchResponse::from(BTreeMap::new());
        }

        let read_result = read_result.unwrap();
        let total_size = read_result
            .values()
            .map(|log_fetch_info| log_fetch_info.records.size() as i32)
            .sum::<i32>();

        if total_size > request.min_bytes {
            FetchResponse::from(read_result)
        } else {
            // 如果读取到的消息小于min_bytes，则将请求加入到delayed_fetch_purgatory中
            let position_infos = read_result
                .iter()
                .map(|(tp, log_fetch_info)| (tp.clone(), log_fetch_info.position_info))
                .collect::<BTreeMap<_, _>>();

            let (tx, rx) =
                tokio::sync::oneshot::channel::<BTreeMap<TopicPartition, LogFetchInfo>>();
            let delay_fetch_keys: Vec<String> = request
                .fetch_data
                .keys()
                .map(|topic_partition| topic_partition.to_string())
                .collect();
            let delayed_fetch = DelayedFetch::new(request, self.clone(), position_infos, tx);
            let delayed_fetch_clone = Arc::new(delayed_fetch);
            self.delayed_fetch_purgatory
                .try_complete_else_watch(delayed_fetch_clone, delay_fetch_keys)
                .await;
            let result = rx.await.unwrap();

            FetchResponse::from(result)
        }
    }

    pub async fn do_fetch(
        &self,
        request: &FetchRequest,
    ) -> AppResult<BTreeMap<TopicPartition, LogFetchInfo>> {
        let mut limit = request.max_bytes;
        let mut read_result = BTreeMap::new();

        for (topic_partition, partition_data) in &request.fetch_data {
            let queue_partition = self.all_queue_partitions.get(topic_partition).unwrap();
            let log_fetch_info = queue_partition
                .read_records(partition_data.fetch_offset, limit)
                .await?;
            limit = (limit - log_fetch_info.records.size() as i32).max(0);
            read_result.insert(topic_partition.clone(), log_fetch_info);
        }

        Ok(read_result)
    }

    pub async fn get_leo_info(&self, tp: &TopicPartition) -> AppResult<PositionInfo> {
        let queue_partition = self.all_queue_partitions.get(tp).unwrap();
        let queue_partition_clone = queue_partition.clone();
        drop(queue_partition);
        let leo_info = queue_partition_clone.get_leo_info().await?;
        Ok(leo_info)
    }
}
