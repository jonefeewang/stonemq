mod delayed_fetch;
mod partation_appender;
mod read;
mod replica_manager;

pub use partation_appender::PartitionAppender;

use crate::log::JournalLog;
use crate::log::LogManager;
use crate::log::PositionInfo;
use crate::log::QueueLog;
use crate::message::LogFetchInfo;
use crate::message::{JournalPartition, QueuePartition, TopicPartition};
use crate::request::FetchRequest;
use crate::utils::DelayedAsyncOperationPurgatory;
use crossbeam::atomic::AtomicCell;
use dashmap::DashMap;
use once_cell::sync::OnceCell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct JournalReplica {
    pub log: Arc<JournalLog>,
}

impl JournalReplica {
    pub fn new(log: Arc<JournalLog>) -> Self {
        Self { log }
    }
}
#[derive(Debug)]
pub struct QueueReplica {
    pub log: Arc<QueueLog>,
}
impl QueueReplica {
    pub fn new(log: Arc<QueueLog>) -> Self {
        Self { log }
    }
}

/// replica manager 持有一个all partitions的集合，这个合是从controller发送的
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
    _shutdown_complete_tx: Sender<()>,
    delayed_fetch_purgatory: Arc<DelayedAsyncOperationPurgatory<DelayedFetch>>,
    partition_appender: PartitionAppender,
}
type FetchResultSender = oneshot::Sender<BTreeMap<TopicPartition, LogFetchInfo>>;
#[derive(Debug)]
pub struct DelayedFetch {
    pub replica_manager: Arc<ReplicaManager>,
    pub request: FetchRequest,
    pub read_position_infos: BTreeMap<TopicPartition, PositionInfo>,
    pub tx: Arc<Mutex<Option<FetchResultSender>>>,
    is_completed: AtomicCell<bool>,
}
