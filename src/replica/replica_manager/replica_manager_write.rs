// Copyright 2025 jonefeewang@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

use tracing::error;

use crate::{
    log::{LogAppendInfo, DEFAULT_LOG_APPEND_TIME},
    message::{JournalPartition, QueuePartition, TopicData, TopicPartition},
    replica::{JournalReplica, QueueReplica},
    request::{ErrorCode, KafkaError, PartitionResponse},
    AppError, LogType,
};

use super::ReplicaManager;

impl ReplicaManager {
    pub async fn append_records(
        &self,
        topics_data: Vec<TopicData>,
    ) -> BTreeMap<TopicPartition, PartitionResponse> {
        let error_response =
            |topic_partition: &TopicPartition, error_code: i16| PartitionResponse {
                partition: topic_partition.partition(),
                error_code,
                base_offset: 0,
                log_append_time: DEFAULT_LOG_APPEND_TIME,
            };
        let mut tp_response = BTreeMap::new();
        for topic_data in topics_data {
            let topic_name = &topic_data.topic_name;
            for partition in topic_data.partition_data {
                let queue_topic_partition =
                    TopicPartition::new_queue(topic_name.clone(), partition.partition);

                let journal_tp = {
                    let journal_tp = self.queue_2_journal.get(&queue_topic_partition);
                    if journal_tp.is_none() {
                        tp_response.insert(
                            queue_topic_partition.clone(),
                            error_response(
                                &queue_topic_partition,
                                ErrorCode::UnknownTopicOrPartition as i16,
                            ),
                        );
                        continue;
                    }
                    journal_tp.unwrap().clone()
                };

                let append_result = self
                    .partition_appender
                    .append_journal(&journal_tp, &queue_topic_partition, partition.message_set)
                    .await;

                if let Ok(LogAppendInfo { first_offset, .. }) = append_result {
                    tp_response.insert(
                        queue_topic_partition.clone(),
                        PartitionResponse {
                            partition: partition.partition,
                            error_code: 0,
                            base_offset: first_offset,
                            log_append_time: DEFAULT_LOG_APPEND_TIME,
                        },
                    );
                } else if let Err(e) = append_result {
                    error!("append records error: {:?}", e);
                    tp_response.insert(
                        queue_topic_partition.clone(),
                        error_response(
                            &queue_topic_partition,
                            ErrorCode::from(&KafkaError::from(e)) as i16,
                        ),
                    );
                }
            }
        }
        for (tp, _) in tp_response.iter() {
            self.delayed_fetch_purgatory
                .check_and_complete(tp.to_string().as_str())
                .await;
        }
        tp_response
    }

    pub fn create_journal_partitions(
        &mut self,
        broker_id: i32,
        tp_strs: &Vec<String>,
    ) -> Result<Vec<(TopicPartition, JournalPartition)>, AppError> {
        let mut partitions = Vec::with_capacity(tp_strs.len());
        for tp_str in tp_strs {
            if tp_str.trim().is_empty() {
                continue;
            }
            let topic_partition = TopicPartition::from_str(tp_str, LogType::Journal)?;
            // setup initial metadata cache
            self.journal_metadata_cache
                .entry(topic_partition.topic().to_string())
                .or_default()
                .insert(topic_partition.partition());

            // get the corresponding log, if not, create one
            let log = self
                .log_manager
                .get_or_create_journal_log(&topic_partition)?;
            let replica = JournalReplica::new(log);
            let partition = JournalPartition::new(topic_partition.clone());
            partition.create_replica(broker_id, replica);
            partitions.push((topic_partition, partition));
        }
        Ok(partitions)
    }
    pub fn create_queue_partitions(
        &self,
        broker_id: i32,
        tp_strs: &Vec<String>,
    ) -> Result<Vec<(TopicPartition, QueuePartition)>, AppError> {
        let mut partitions = Vec::with_capacity(tp_strs.len());
        for tp_str in tp_strs {
            if tp_str.trim().is_empty() {
                continue;
            }
            let topic_partition = TopicPartition::from_str(tp_str, LogType::Queue)?;
            // setup initial metadata cache
            self.queue_metadata_cache
                .entry(topic_partition.topic().to_string())
                .or_default()
                .insert(topic_partition.partition());

            let log = self.log_manager.get_or_create_queue_log(&topic_partition)?;
            let replica = QueueReplica::new(log);
            let partition = QueuePartition::new(topic_partition.clone());
            partition.create_replica(broker_id, replica);
            partitions.push((topic_partition, partition));
        }
        Ok(partitions)
    }
}
