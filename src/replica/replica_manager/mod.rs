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

mod replica_manager_read;
mod replica_manager_write;

use super::{DelayedFetch, PartitionAppender};
use crate::log::LogManager;
use crate::message::{JournalPartition, QueuePartition, TopicPartition};
use crate::utils::{DelayedAsyncOperationPurgatory, WorkerPoolConfig};
use crate::{global_config, AppResult, Shutdown};
use dashmap::DashMap;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender;
use tracing::{debug, error, info};

/// replica manager holds a set of all partitions, which is obtained from the
/// leaderAndIsrRequest command sent by the controller. All replica information
/// is contained in the partitions. The topic partition and partition here are
/// not merged for consistency, each manages its own. The replica manager
/// manages the storage layer through the log manager.
#[derive(Debug)]
pub struct ReplicaManager {
    all_journal_partitions: DashMap<TopicPartition, Arc<JournalPartition>>,
    all_queue_partitions: DashMap<TopicPartition, Arc<QueuePartition>>,
    queue_2_journal: DashMap<TopicPartition, TopicPartition>,
    pub(crate) log_manager: Arc<LogManager>,
    journal_metadata_cache: DashMap<String, BTreeSet<i32>>,
    queue_metadata_cache: DashMap<String, BTreeSet<i32>>,
    notify_shutdown: broadcast::Sender<()>,
    _shutdown_complete_tx: Sender<()>,
    delayed_fetch_purgatory: Arc<DelayedAsyncOperationPurgatory<DelayedFetch>>,
    partition_appender: PartitionAppender,
}

impl ReplicaManager {
    pub async fn new(
        log_manager: Arc<LogManager>,
        notify_shutdown: broadcast::Sender<()>,
        _shutdown_complete_tx: Sender<()>,
    ) -> Self {
        let notify_shutdown_clone = notify_shutdown.clone();
        let delayed_fetch_purgatory = DelayedAsyncOperationPurgatory::<DelayedFetch>::new(
            "delayed_fetch_purgatory",
            notify_shutdown_clone,
        )
        .await;

        let partition_appender = Self::create_partition_appender(notify_shutdown.clone());

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
            partition_appender,
        }
    }
    fn create_partition_appender(notify_shutdown: broadcast::Sender<()>) -> PartitionAppender {
        let worker_pool_config = WorkerPoolConfig {
            channel_capacity: global_config().partition_appender_pool.channel_capacity,
            monitor_interval: Duration::from_secs(
                global_config().partition_appender_pool.monitor_interval,
            ),
            num_channels: global_config().partition_appender_pool.num_channels,
            worker_check_timeout: Duration::from_millis(
                global_config().partition_appender_pool.worker_check_timeout,
            ),
        };

        PartitionAppender::new(notify_shutdown, Some(worker_pool_config))
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
            self.partition_appender
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
        let journal_count = global_config().log.journal_topic_count;
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
}

impl Drop for ReplicaManager {
    fn drop(&mut self) {
        debug!("replica manager dropped");
    }
}
