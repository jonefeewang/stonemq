use crate::log::LogAppendInfo;
use crate::message::delayed_fetch::DelayedFetch;
use crate::message::topic_partition::{JournalPartition, QueuePartition};
use crate::message::{TopicData, TopicPartition};
use crate::protocol::{ProtocolError, INVALID_TOPIC_ERROR};
use crate::request::produce::PartitionResponse;
use crate::utils::{DelayedAsyncOperationPurgatory, JOURNAL_TOPICS_LIST, QUEUE_TOPICS_LIST};
use crate::AppError::{IllegalStateError, InvalidValue};
use crate::{global_config, AppError, AppResult, KvStore, LogManager, Shutdown};
use dashmap::DashMap;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender;
use tracing::{error, info, trace};

use super::{JournalReplica, QueueReplica, ReplicaManager};

impl ReplicaManager {
    pub fn new(
        log_manager: Arc<LogManager>,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: Sender<()>,
        rt: &Runtime,
    ) -> Self {
        let notify_shutdown_clone = notify_shutdown.clone();
        let shutdown_complete_tx_clone = shutdown_complete_tx.clone();
        let delayed_fetch_purgatory = rt.block_on(async move {
            DelayedAsyncOperationPurgatory::<DelayedFetch>::new(
                "delayed_fetch_purgatory",
                notify_shutdown_clone,
                shutdown_complete_tx_clone,
            )
            .await
        });
        ReplicaManager {
            log_manager,
            all_journal_partitions: DashMap::new(),
            all_queue_partitions: DashMap::new(),
            queue_2_journal: DashMap::new(),
            journal_metadata_cache: DashMap::new(),
            queue_metadata_cache: DashMap::new(),
            notify_shutdown,
            shutdown_complete_tx,
            delayed_fetch_purgatory,
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

                // 一次循环形成一个dashmap的entry value的作用域，尽快释放读锁
                let journal_tp =
                    self.queue_2_journal
                        .get(&topic_partition)
                        .ok_or(IllegalStateError(Cow::Owned(format!(
                            "No corresponding journal partition.:{}",
                            topic_partition
                        ))))?;
                let journal_tp_clone = journal_tp.clone();
                drop(journal_tp);
                let journal_partition =
                    self.all_journal_partitions
                        .get(&journal_tp_clone)
                        .ok_or(IllegalStateError(Cow::Owned(format!(
                            "corresponding journal partition not found.:{}",
                            journal_tp_clone
                        ))))?;
                // 这里没做journal_partition的复制，占用了一下dashmap的锁，不过问题不大，因为你真正的底层耗时操作log.append之前已经
                // 释放了读锁
                let LogAppendInfo { first_offset, .. } = journal_partition
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
                        base_offset: first_offset,
                        log_append_time: None,
                    },
                );
            }
        }
        for (tp, _) in tp_response.iter() {
            self.delayed_fetch_purgatory
                .check_and_complete(tp.to_string().as_str())
                .await;
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
        let kv_store_file_path = &global_config().log.kv_store_path;
        let broker_id = global_config().general.id;
        let kv_store = KvStore::open(kv_store_file_path)?;
        // load journal partitions
        // In cluster mode, this list should be obtained from the controller
        // Here, a simulated journal topic list includes: journal-0, journal-1
        let journal_tps = kv_store
            .get(JOURNAL_TOPICS_LIST)
            .ok_or(InvalidValue("journal_topics_list", String::new()))?;
        let tp_strs: Vec<&str> = journal_tps.split(',').map(|token| token.trim()).collect();
        let mut partitions = self.create_journal_partitions(broker_id, tp_strs, rt)?;
        self.all_journal_partitions.extend(
            partitions
                .drain(..)
                .map(|(tp, partition)| (tp, Arc::new(partition))),
        );
        info!(
            "load journal partitions: {:?}",
            self.all_journal_partitions
                .iter()
                .map(|entry| { entry.key().clone().id() })
                .collect::<Vec<String>>()
        );

        // load queue partitions
        // Here, simulate 10 queue partitions (with 5 queue partitions allocated to each journal).
        let queue_tps = kv_store
            .get(QUEUE_TOPICS_LIST)
            .ok_or(InvalidValue("queue_topics_list", String::new()))?;
        let tp_strs: Vec<&str> = queue_tps.split(',').collect();
        let mut partitions = self.create_queue_partitions(broker_id, tp_strs, rt)?;
        self.all_queue_partitions.extend(
            partitions
                .drain(..)
                .map(|(tp, partition)| (tp, Arc::new(partition))),
        );
        info!(
            "load queue partitions: {:?}",
            self.all_queue_partitions
                .iter()
                .map(|entry| { entry.key().clone().id() })
                .collect::<Vec<String>>()
        );

        // 确定journal和queue的对应关系
        // 获取实际的 JournalLog 数量
        let journal_log_count = self.all_journal_partitions.len();

        if journal_log_count == 0 {
            return Err(AppError::IllegalStateError("没有可用的 JournalLog".into()));
        }

        // 使用迭代器索引来实现轮询
        self.all_queue_partitions
            .iter()
            .enumerate()
            .for_each(|(index, entry)| {
                let journal_partition = index % journal_log_count;
                let journal_topic = TopicPartition {
                    topic: "journal".to_string(),
                    partition: journal_partition as i32,
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
                        entry.key().clone().id(),
                        entry.value().clone().id()
                    )
                })
                .collect::<Vec<String>>()
        );

        // 启动journal log splitter
        // Create a BTreeMap to store the reverse mapping
        let mut journal_to_queues: BTreeMap<TopicPartition, HashSet<TopicPartition>> =
            BTreeMap::new();

        // Iterate over the DashMap and build the reverse mapping
        for entry in self.queue_2_journal.iter() {
            let queue_tp = entry.key().clone();
            let journal_tp = entry.value().clone();

            journal_to_queues
                .entry(journal_tp)
                .or_default()
                .insert(queue_tp);
        }

        // 启动journal log splitter
        for (journal_tp, queue_tps) in journal_to_queues {
            let shutdown = Shutdown::new(self.notify_shutdown.subscribe());
            let log_manager = self.log_manager.clone();
            rt.spawn(async move {
                if let Err(e) = log_manager
                    .start_splitter_task(journal_tp, queue_tps, shutdown)
                    .await
                {
                    error!("Splitter task error: {:?}", e);
                }
            });
        }
        info!("ReplicaManager startup completed.");
        Ok(())
    }

    fn create_journal_partitions(
        &mut self,
        broker_id: i32,
        tp_strs: Vec<&str>,
        rt: &Runtime,
    ) -> Result<Vec<(TopicPartition, JournalPartition)>, AppError> {
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
            let replica = JournalReplica::new(broker_id, topic_partition.clone(), log);
            let partition = JournalPartition::new(topic_partition.clone());
            partition.create_replica(broker_id, replica);
            partitions.push((topic_partition, partition));
        }
        Ok(partitions)
    }
    fn create_queue_partitions(
        &self,
        broker_id: i32,
        tp_strs: Vec<&str>,
        rt: &Runtime,
    ) -> Result<Vec<(TopicPartition, QueuePartition)>, AppError> {
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
            let replica = QueueReplica::new(broker_id, topic_partition.clone(), log);
            let partition = QueuePartition::new(topic_partition.clone());
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
    pub fn get_journal_partition(
        &self,
        queue_topic_partition: &TopicPartition,
    ) -> Option<TopicPartition> {
        self.queue_2_journal
            .get(queue_topic_partition)
            .map(|tp| tp.clone())
    }
}
