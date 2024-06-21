use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::sync::Arc;

use dashmap::DashMap;
use tokio::runtime::Runtime;
use tracing::{info, trace};

use crate::{AppError, AppResult, BROKER_CONFIG, KvStore, LogManager, QueueLog};
use crate::AppError::{IllegalStateError, InvalidValue};
use crate::log::{JournalLog, Log};
use crate::protocol::{INVALID_TOPIC_ERROR, ProtocolError};
use crate::request::produce::PartitionResponse;
use crate::topic_partition::{LogAppendInfo, Partition, TopicData, TopicPartition};
use crate::utils::mini_kv_db::{JOURNAL_TOPICS_LIST, QUEUE_TOPICS_LIST};

#[derive(Debug)]
pub struct Replica<T: Log> {
    pub broker_id: i32,
    pub topic_partition: TopicPartition,
    pub log: Arc<T>,
}
impl<T: Log> Replica<T> {
    pub fn new(broker_id: i32, topic_partition: TopicPartition, log: Arc<T>) -> Self {
        Replica {
            broker_id,
            topic_partition,
            log,
        }
    }
}
/// replica manager 持有一个all partitions的集合，这个集合是从controller发送的
/// leaderAndIsrRequest命令里获取的, 所有的replica信息都在partition里。Log里的
/// topic partition 和 这里的partition没有做一致性的合并，各自管理各自的。replica manager
/// 通过log manager来管理存储层
#[derive(Debug)]
pub struct ReplicaManager {
    all_journal_partitions: DashMap<TopicPartition, Partition<JournalLog>>,
    all_queue_partitions: DashMap<TopicPartition, Partition<QueueLog>>,
    queue_2_journal: DashMap<TopicPartition, TopicPartition>,
    log_manager: LogManager,
    journal_metadata_cache: DashMap<String, BTreeSet<i32>>,
    queue_metadata_cache: DashMap<String, BTreeSet<i32>>,
}

impl ReplicaManager {
    pub fn new(log_manager: LogManager) -> Self {
        ReplicaManager {
            log_manager,
            all_journal_partitions: DashMap::new(),
            all_queue_partitions: DashMap::new(),
            queue_2_journal: DashMap::new(),
            journal_metadata_cache: DashMap::new(),
            queue_metadata_cache: DashMap::new(),
        }
    }
    pub async fn append_records(
        &self,
        topics_data: Vec<TopicData>,
    ) -> AppResult<BTreeMap<TopicPartition, PartitionResponse>> {
        let mut tp_response = BTreeMap::new();
        for topic_data in topics_data {
            let topic_name = &topic_data.topic_name;
            for partition in topic_data.partition_data {
                let topic_partition = TopicPartition {
                    topic: topic_name.clone(),
                    partition: partition.partition,
                };
                let journal_partition =
                    self.queue_2_journal
                        .get(&topic_partition)
                        .ok_or(IllegalStateError(Cow::Owned(format!(
                            "No corresponding journal partition.:{}",
                            topic_partition
                        ))))?;
                let journal_partition = self
                    .all_journal_partitions
                    .get(journal_partition.deref())
                    .ok_or(IllegalStateError(Cow::Owned(format!(
                        "corresponding journal partition not found.:{}",
                        *journal_partition
                    ))))?;
                let LogAppendInfo { base_offset, .. } = journal_partition
                    .append_record_to_leader(partition.message_set, topic_partition)
                    .await?;

                tp_response.insert(
                    TopicPartition {
                        topic: topic_name.clone(),
                        partition: partition.partition,
                    },
                    PartitionResponse {
                        partition: partition.partition,
                        error_code: 0,
                        base_offset,
                        log_append_time: None,
                    },
                );
            }
        }
        Ok(tp_response)
    }
    ///
    /// Since the current StoneMQ operates on a single-machine for production and consumption,
    /// without a cluster concept, retrieve all topic partition information from the kv database here,
    /// simulating how it would be obtained from the leader and ISR request sent by the controller.
    ///
    pub fn startup(&mut self, rt: &Runtime) -> AppResult<()> {
        info!("ReplicaManager starting up...");
        // load all partitions from kv db
        let kv_store_file_path = &BROKER_CONFIG.get().unwrap().log.kv_store_path;
        let broker_id = BROKER_CONFIG.get().unwrap().general.id;
        let kv_store = KvStore::open(kv_store_file_path)?;
        // load journal partitions
        // In cluster mode, this list should be obtained from the controller
        // Here, a simulated journal topic list includes: journal-0, journal-1
        let journal_tps = kv_store
            .get(JOURNAL_TOPICS_LIST)
            .ok_or(InvalidValue("journal_topics_list", String::new()))?;
        let tp_strs: Vec<&str> = journal_tps.split(',').map(|token| token.trim()).collect();
        let mut partitions = self.create_journal_partitions(broker_id, tp_strs, rt)?;
        self.all_journal_partitions.extend(partitions.drain(..));
        info!(
            "load journal partitions: {:?}",
            self.all_journal_partitions
                .iter()
                .map(|entry| { entry.key().clone().string_id() })
                .collect::<Vec<String>>()
        );

        // load queue partitions
        // Here, simulate 10 queue partitions (with 5 queue partitions allocated to each journal).
        let queue_tps = kv_store
            .get(QUEUE_TOPICS_LIST)
            .ok_or(InvalidValue("queue_topics_list", String::new()))?;
        let tp_strs: Vec<&str> = queue_tps.split(',').collect();
        let mut partitions = self.create_queue_partitions(broker_id, tp_strs, rt)?;
        self.all_queue_partitions.extend(partitions.drain(..));
        info!(
            "load queue partitions: {:?}",
            self.all_queue_partitions
                .iter()
                .map(|entry| { entry.key().clone().string_id() })
                .collect::<Vec<String>>()
        );

        // 确定journal和queue的对应关系
        self.all_queue_partitions.iter().for_each(|entry| {
            let journal_topic = TopicPartition {
                topic: "journal".to_string(),
                partition: entry.topic_partition.partition % 2,
            };
            self.queue_2_journal
                .insert(entry.topic_partition.clone(), journal_topic);
        });

        info!(
            "queue to journal map:{:?}",
            self.queue_2_journal
                .iter()
                .map(|entry| {
                    format!(
                        "{} -> {}",
                        entry.key().clone().string_id(),
                        entry.value().clone().string_id()
                    )
                })
                .collect::<Vec<String>>()
        );

        info!("ReplicaManager startup completed.");
        Ok(())
    }

    fn create_journal_partitions(
        &mut self,
        broker_id: i32,
        tp_strs: Vec<&str>,
        rt: &Runtime,
    ) -> Result<Vec<(TopicPartition, Partition<JournalLog>)>, AppError> {
        let mut partitions = Vec::with_capacity(tp_strs.len());
        for tp_str in tp_strs {
            if tp_str.trim().is_empty() {
                continue;
            }
            let topic_partition = TopicPartition::from_string(Cow::Owned(tp_str.to_string()))?;
            // setup initial metadata cache
            self.journal_metadata_cache
                .entry(topic_partition.topic.clone())
                .or_default()
                .insert(topic_partition.partition);

            // 获取对应的log，没有的话，创建一个
            let log = self
                .log_manager
                .get_or_create_journal_log(&topic_partition, rt)?;
            let replica = Replica::new(broker_id, topic_partition.clone(), log);
            let mut partition = Partition::new(topic_partition.clone());
            partition.create_replica(broker_id, replica);
            partitions.push((topic_partition, partition));
        }
        Ok(partitions)
    }
    fn create_queue_partitions(
        &mut self,
        broker_id: i32,
        tp_strs: Vec<&str>,
        rt: &Runtime,
    ) -> Result<Vec<(TopicPartition, Partition<QueueLog>)>, AppError> {
        let mut partitions = Vec::with_capacity(tp_strs.len());
        for tp_str in tp_strs {
            if tp_str.trim().is_empty() {
                continue;
            }
            let topic_partition = TopicPartition::from_string(Cow::Owned(tp_str.to_string()))?;
            // setup initial metadata cache
            self.queue_metadata_cache
                .entry(topic_partition.topic.clone())
                .or_default()
                .insert(topic_partition.partition);

            let log = self
                .log_manager
                .get_or_create_queue_log(&topic_partition, rt)?;
            let replica = Replica::new(broker_id, topic_partition.clone(), log);
            let mut partition = Partition::new(topic_partition.clone());
            partition.create_replica(broker_id, replica);
            partitions.push((topic_partition, partition));
        }
        Ok(partitions)
    }
    ///
    /// 返回(主题名称, 主题分区列表, 协议错误)
    pub fn get_queue_metadata(
        &self,
        topics: Option<Vec<Cow<str>>>,
    ) -> Vec<(String, Option<Vec<i32>>, Option<ProtocolError>)> {
        match topics {
            None => {
                // 返回所有主题

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
                // 返回客户端要求的主题
                topics
                    .iter()
                    .map(|topic| {
                        let topic = topic.as_ref();
                        let partitions = self.queue_metadata_cache.get(topic);
                        match partitions {
                            None => (
                                topic.to_string(),
                                None,
                                Some(ProtocolError::InvalidTopic(INVALID_TOPIC_ERROR)),
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
}
