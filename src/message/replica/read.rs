use std::collections::BTreeMap;
use std::sync::Arc;

use super::ReplicaManager;
use crate::message::delayed_fetch::DelayedFetch;
use crate::message::{LogFetchInfo, TopicPartition};
use crate::request::fetch::{FetchRequest, FetchResponse};
use crate::AppResult;

impl ReplicaManager {
    pub async fn fetch_message(
        self: Arc<ReplicaManager>,
        request: FetchRequest,
    ) -> AppResult<FetchResponse> {
        let read_result = self.do_fetch(&request).await?;

        let total_size = read_result
            .iter()
            .map(|(_, log_fetch_info)| log_fetch_info.records.size() as i32)
            .sum::<i32>();

        if total_size > request.min_bytes {
            Ok(FetchResponse::from(read_result))
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
            let delayed_fetch = DelayedFetch {
                position_infos,
                tx,
                request,
                replica_manager: self.clone(),
            };

            self.delayed_fetch_purgatory
                .try_complete_else_watch(&Arc::new(delayed_fetch), delay_fetch_keys)
                .await;
            let result = rx.await.unwrap();

            Ok(FetchResponse::from(result))
        }
    }

    async fn do_fetch(
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
}
