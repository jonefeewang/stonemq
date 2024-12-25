use super::{global_journal_partition_appender, DelayedFetch};
use crate::log::{LogAppendInfo, DEFAULT_LOG_APPEND_TIME};
use crate::log::{LogManager, LogType};
use crate::message::{JournalPartition, QueuePartition, TopicData, TopicPartition};
use crate::request::{ErrorCode, KafkaError, PartitionResponse};
use crate::utils::DelayedAsyncOperationPurgatory;
use crate::{global_config, AppError, AppResult, Shutdown};
use dashmap::DashMap;
use rocksdb::DB;

use std::collections::{BTreeMap, HashSet};

use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info};

use super::{JournalReplica, QueueReplica, ReplicaManager};

impl ReplicaManager {
    const KEY_QUEUE_TOPICS: &str = "queue_topics";
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

                let append_result = global_journal_partition_appender()
                    .append_journal(&topic_partition, &journal_tp_clone, partition.message_set)
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

        let broker_id = global_config().general.id;
        let journal_tps = self.get_journal_topics();
        let mut partitions = self.create_journal_partitions(broker_id, &journal_tps)?;
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

        // register journal partitions to the appender
        self.all_journal_partitions.iter().for_each(|entry| {
            global_journal_partition_appender()
                .register_partition(entry.key().clone(), entry.value().clone());
        });

        // load queue partitions
        // Here, simulate 10 queue partitions (with 5 queue partitions allocated to each journal).
        let queue_tps = self.get_queue_topics();

        let mut partitions = self.create_queue_partitions(broker_id, &queue_tps)?;
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

        // establish the relationship between journal and queue
        // get the actual JournalLog count
        let journal_count = global_config().log.journal_count;
        let mut sorted_queue_partitions = self
            .all_queue_partitions
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        sorted_queue_partitions.sort_by_key(|entry| entry.id());

        sorted_queue_partitions
            .iter()
            .enumerate()
            .for_each(|(index, entry)| {
                let journal_partition = index % journal_count as usize;
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

        // start journal log splitter
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

        // start journal log splitter
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
    fn create_queue_partitions(
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
    fn get_journal_topics(&self) -> Vec<String> {
        let journal_count = global_config().log.journal_count;
        let mut topics = Vec::with_capacity(journal_count as usize);
        for i in 0..journal_count {
            topics.push(format!("journal-{}", i));
        }
        topics
    }

    fn get_queue_topics(&self) -> Vec<String> {
        let path = &global_config().general.local_db_path;
        let db = DB::open_default(path).unwrap();
        let topics = db.get(Self::KEY_QUEUE_TOPICS).unwrap().unwrap();
        let topics: Vec<String> = String::from_utf8(topics)
            .unwrap()
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        topics
    }
}

impl Drop for ReplicaManager {
    fn drop(&mut self) {
        debug!("replica manager dropped");
    }
}
