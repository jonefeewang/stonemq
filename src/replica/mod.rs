mod delayed_fetch;
mod partation_appender;
mod replica_manager;

pub use partation_appender::PartitionAppender;
pub use replica_manager::ReplicaManager;

use crate::log::JournalLog;
use crate::log::PositionInfo;
use crate::log::QueueLog;
use crate::message::LogFetchInfo;
use crate::message::TopicPartition;
use crate::request::FetchRequest;
use crossbeam::atomic::AtomicCell;
use std::collections::BTreeMap;
use std::sync::Arc;

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

type FetchResultSender = oneshot::Sender<BTreeMap<TopicPartition, LogFetchInfo>>;
#[derive(Debug)]
pub struct DelayedFetch {
    pub replica_manager: Arc<ReplicaManager>,
    pub request: FetchRequest,
    pub read_position_infos: BTreeMap<TopicPartition, PositionInfo>,
    pub tx: Arc<Mutex<Option<FetchResultSender>>>,
    is_completed: AtomicCell<bool>,
}
