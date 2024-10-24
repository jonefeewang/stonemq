mod manager;
mod read;

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::log::{JournalLog, Log, QueueLog};
use crate::message::delayed_fetch::DelayedFetch;
use crate::message::delayed_operation::{DelayedOperation, DelayedOperationPurgatory};
use crate::message::TopicPartition;
use crate::LogManager;
use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender;

use super::topic_partition::{JournalPartition, QueuePartition};

#[derive(Debug)]
pub struct Replica<L: Log> {
    pub broker_id: i32,
    pub topic_partition: TopicPartition,
    pub log: Arc<L>,
}

impl<L: Log> Replica<L> {
    pub fn new(broker_id: i32, topic_partition: TopicPartition, log: Arc<L>) -> Self {
        Replica {
            broker_id,
            topic_partition,
            log,
        }
    }
}

pub type JournalReplica = Replica<JournalLog>;
pub type QueueReplica = Replica<QueueLog>;

/// replica manager 持有一个all partitions的集合，这个���合是从controller发送的
/// leaderAndIsrRequest命令里获取的, 所有的replica信息都在partition里。Log里的
/// topic partition 和 这里的partition没有做一致性的合并，各自管理各自的。replica manager
/// 通过log manager来管理存储层
#[derive(Debug)]
pub struct ReplicaManager {
    all_journal_partitions: DashMap<TopicPartition, Arc<JournalPartition>>,
    all_queue_partitions: DashMap<TopicPartition, Arc<QueuePartition>>,
    queue_2_journal: DashMap<TopicPartition, TopicPartition>,
    pub(crate) log_manager: Arc<LogManager>,
    journal_metadata_cache: DashMap<String, BTreeSet<i32>>,
    queue_metadata_cache: DashMap<String, BTreeSet<i32>>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: Sender<()>,
    delayed_fetch_purgatory: Arc<DelayedOperationPurgatory<DelayedFetch>>,
}
