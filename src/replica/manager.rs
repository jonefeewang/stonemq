use super::DelayedFetch;
use crate::log::{JournalLog, LogManager, LogType};
use crate::log::{LogAppendInfo, DEFAULT_LOG_APPEND_TIME};
use crate::message::{JournalPartition, QueuePartition, TopicData, TopicPartition};
use crate::request::{ErrorCode, KafkaError, PartitionResponse};
use crate::utils::{
    DelayedAsyncOperationPurgatory, MultipleChannelWorkerPool, WorkerPoolConfig,
    JOURNAL_TOPICS_LIST, QUEUE_TOPICS_LIST,
};
use crate::{global_config, AppError, AppResult, KvStore, MemoryRecords, Shutdown};
use dashmap::DashMap;

use std::collections::{BTreeMap, HashSet};

use std::sync::Arc;

use tokio::sync::mpsc::Sender;
use tokio::sync::{broadcast, oneshot};
use tracing::{debug, error, info};

use super::{JournalReplica, QueueReplica, ReplicaManager};

impl ReplicaManager {
    pub async fn new(
        log_manager: Arc<LogManager>,
        notify_shutdown: broadcast::Sender<()>,
        _shutdown_complete_tx: Sender<()>,
    ) -> Self {
        let notify_shutdown_clone = notify_shutdown.clone();
        let shutdown_complete_tx_clone = _shutdown_complete_tx.clone();
        let delayed_fetch_purgatory = DelayedAsyncOperationPurgatory::<DelayedFetch>::new(
            "delayed_fetch_purgatory",
            notify_shutdown_clone,
            shutdown_complete_tx_clone,
        )
        .await;

        ReplicaManager {
            log_manager,
            all_journal_partitions: DashMap::new(),
            all_queue_partitions: DashMap::new(),
            queue_2_journal: DashMap::new(),
            journal_metadata_cache: DashMap::new(),
            queue_metadata_cache: DashMap::new(),
            notify_shutdown,
            _shutdown_complete_tx,
            delayed_fetch_purgatory,
        }
    }
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
                let topic_partition =
                    TopicPartition::new_journal(topic_name.clone(), partition.partition);

                let journal_tp_clone = {
                    let journal_tp = self.queue_2_journal.get(&topic_partition);
                    if journal_tp.is_none() {
                        tp_response.insert(
                            topic_partition.clone(),
                            error_response(
                                &topic_partition,
                                ErrorCode::UnknownTopicOrPartition as i16,
                            ),
                        );
                        continue;
                    }
                    journal_tp.unwrap().clone()
                };

                let journal_partition = {
                    let journal_partition = self.all_journal_partitions.get(&journal_tp_clone);
                    if journal_partition.is_none() {
                        tp_response.insert(
                            topic_partition.clone(),
                            error_response(
                                &topic_partition,
                                ErrorCode::UnknownTopicOrPartition as i16,
                            ),
                        );
                        continue;
                    }
                    journal_partition.unwrap().clone()
                };
                let append_result = journal_partition
                    .append_record_to_leader(
                        partition.message_set,
                        topic_partition.clone(),
                        &global_active_log_file_writer().journal_prepare_pool,
                    )
                    .await;
                if let Ok(LogAppendInfo { first_offset, .. }) = append_result {
                    tp_response.insert(
                        topic_partition.clone(),
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
                        topic_partition.clone(),
                        error_response(
                            &topic_partition,
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
    ///
    /// Since the current StoneMQ operates on a single-machine for production and consumption,
    /// without a cluster concept, retrieve all topic partition information from the kv database here,
    /// simulating how it would be obtained from the leader and ISR request sent by the controller.
    ///
    pub fn startup(&mut self) -> AppResult<()> {
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
            .ok_or(AppError::InvalidValue(
                "kv config journal_topics_list".to_string(),
            ))?;
        let tp_strs: Vec<&str> = journal_tps.split(',').map(|token| token.trim()).collect();
        let mut partitions = self.create_journal_partitions(broker_id, tp_strs)?;
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
            .ok_or(AppError::InvalidValue(
                "kv config queue_topics_list".to_string(),
            ))?;
        let tp_strs: Vec<&str> = queue_tps.split(',').collect();
        let mut partitions = self.create_queue_partitions(broker_id, tp_strs)?;
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

        let mut sorted_queue_partitions = self
            .all_queue_partitions
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        sorted_queue_partitions.sort_by_key(|entry| entry.id());

        // 使用迭代器索引来实现轮询
        sorted_queue_partitions
            .iter()
            .enumerate()
            .for_each(|(index, entry)| {
                let journal_partition = index % journal_log_count;
                let journal_topic =
                    TopicPartition::new_journal("journal".to_string(), journal_partition as i32);
                self.queue_2_journal.insert(entry.clone(), journal_topic);
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
            let shutdown_complete_tx = self._shutdown_complete_tx.clone();
            let log_manager = self.log_manager.clone();
            tokio::spawn(async move {
                if let Err(e) = log_manager
                    .start_splitter_task(journal_tp, queue_tps, shutdown, shutdown_complete_tx)
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

            // 获取对应的log，没有的话，创建一个
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
    fn create_queue_partitions(
        &self,
        broker_id: i32,
        tp_strs: Vec<&str>,
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
    ///
    /// 返回(主题名称, 主题分区列表, 协议错误)
    pub fn get_queue_metadata(
        &self,
        topics: Option<Vec<&str>>,
    ) -> Vec<(String, Option<Vec<i32>>, Option<AppError>)> {
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
}

impl Drop for ReplicaManager {
    fn drop(&mut self) {
        debug!("replica manager dropped");
    }
}

#[derive(Debug)]
pub struct AppendJournalLogReq {
    pub record: MemoryRecords,
    pub queue_topic_partition: TopicPartition,
    pub reply_sender: oneshot::Sender<AppResult<LogAppendInfo>>,
    pub journal_log: Arc<JournalLog>,
}
