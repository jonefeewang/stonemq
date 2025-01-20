use std::collections::BTreeMap;
use std::sync::Arc;

use tracing::debug;

use super::DelayedFetch;
use super::ReplicaManager;
use crate::global_config;
use crate::log::PositionInfo;
use crate::message::{LogFetchInfo, TopicPartition};
use crate::request::{FetchRequest, FetchResponse};

use crate::AppError;
use crate::AppResult;

impl ReplicaManager {
    pub async fn fetch_message(
        self: Arc<ReplicaManager>,
        request: FetchRequest,
        correlation_id: i32,
    ) -> FetchResponse {
        let read_result = self.do_fetch(&request).await;
        if read_result.is_err() {
            debug!("fetch error: {:?}", read_result);
            return self.create_empty_fetch_response(&request);
        }

        let read_result = read_result.unwrap();
        let total_size = read_result
            .values()
            .map(|log_fetch_info| log_fetch_info.records.size() as i32)
            .sum::<i32>();

        if total_size > request.min_bytes {
            debug!("fetch success: {:?}", read_result);
            FetchResponse::from_data(read_result, 0)
        } else {
            // if the message read is less than min_bytes, add the request to the delayed_fetch_purgatory
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
            let delayed_fetch = DelayedFetch::new(request, self.clone(), position_infos, tx,correlation_id);
            let delayed_fetch_clone = Arc::new(delayed_fetch);
            self.delayed_fetch_purgatory
                .try_complete_else_watch(delayed_fetch_clone, delay_fetch_keys)
                .await;
            let result = rx.await.unwrap();
            debug!("fetch success result: {:?}", result);

            FetchResponse::from_data(result, 0)
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

    fn create_empty_fetch_response(&self, request: &FetchRequest) -> FetchResponse {
        let mut read_result = BTreeMap::new();
        for topic_partition in request.fetch_data.keys() {
            let queue_partition = self
                .all_queue_partitions
                .get(topic_partition)
                .unwrap()
                .clone();
            let log_fetch_info = queue_partition.create_empty_fetch_info();
            read_result.insert(topic_partition.clone(), log_fetch_info);
        }
        debug!("create empty fetch response: {:?}", read_result);
        FetchResponse::from_data(read_result, 10000)
    }

    pub fn get_leo_info(&self, tp: &TopicPartition) -> AppResult<PositionInfo> {
        let queue_partition = self.all_queue_partitions.get(tp).unwrap();
        let queue_partition_clone = queue_partition.clone();
        drop(queue_partition);
        let leo_info = queue_partition_clone.get_leo_info()?;
        Ok(leo_info)
    }

    ///
    /// return (topic name, topic partitions, protocol error)
    pub fn get_queue_metadata(
        &self,
        topics: Option<Vec<&str>>,
    ) -> Vec<(String, Option<Vec<i32>>, Option<AppError>)> {
        match topics {
            None => {
                // return all topics
                self.queue_metadata_cache
                    .iter()
                    .map(|entry| {
                        (
                            entry.key().clone(),
                            Some(entry.value().iter().copied().collect()),
                            None,
                        )
                    })
                    .collect()
            }
            Some(topics) => {
                // return the topics requested by the client
                topics
                    .iter()
                    .map(|topic| {
                        let partitions = self.queue_metadata_cache.get(*topic);
                        match partitions {
                            None => (
                                topic.to_string(),
                                None,
                                Some(AppError::InvalidTopic(topic.to_string())),
                            ),
                            Some(partitions) => (
                                topic.to_string(),
                                Some(partitions.iter().copied().collect()),
                                None,
                            ),
                        }
                    })
                    .collect()
            }
        }
    }
    pub fn get_journal_topics(&self) -> Vec<String> {
        let journal_count = global_config().log.journal_topic_count;
        let mut topics = Vec::with_capacity(journal_count as usize);
        for i in 0..journal_count {
            topics.push(format!("journal-{}", i));
        }
        topics
    }

    pub fn get_queue_topics(&self) -> Vec<String> {
        let queue_count = global_config().log.queue_topic_count;
        let mut topics = Vec::with_capacity(queue_count as usize);
        for i in 0..queue_count {
            topics.push(format!("topic_a-{}", i));
        }
        topics
    }
}
