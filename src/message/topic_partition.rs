use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use dashmap::DashMap;
use tracing::trace;

use crate::log::{LogAppendInfo, PositionInfo};
use crate::message::memory_records::MemoryRecords;
use crate::{global_config, AppError, AppResult};

use super::replica::{JournalReplica, QueueReplica};
use super::LogFetchInfo;

#[derive(Debug)]
pub struct JournalPartition {
    pub _topic_partition: TopicPartition,
    pub assigned_replicas: DashMap<i32, Arc<JournalReplica>>,
}
#[derive(Debug)]
pub struct QueuePartition {
    pub topic_partition: TopicPartition,
    pub assigned_replicas: DashMap<i32, Arc<QueueReplica>>,
}

impl JournalPartition {
    pub fn new(topic_partition: TopicPartition) -> Self {
        Self {
            _topic_partition: topic_partition,
            assigned_replicas: DashMap::new(),
        }
    }

    pub async fn append_record_to_leader(
        &self,
        record: MemoryRecords,
        queue_topic_partition: TopicPartition,
    ) -> AppResult<LogAppendInfo> {
        let local_replica_id = global_config().general.id;

        let log = {
            let replica = self.assigned_replicas.get(&local_replica_id);
            if replica.is_none() {
                return Err(AppError::InvalidValue(
                    "replica",
                    local_replica_id.to_string(),
                ));
            }
            replica.unwrap().log.clone()
        };

        log.append_records((queue_topic_partition, record)).await
    }

    pub fn create_replica(&self, broker_id: i32, replica: JournalReplica) {
        self.assigned_replicas.insert(broker_id, Arc::new(replica));
    }
}
impl QueuePartition {
    pub fn new(topic_partition: TopicPartition) -> Self {
        Self {
            topic_partition,
            assigned_replicas: DashMap::new(),
        }
    }

    pub async fn read_records(&self, offset: i64, max_bytes: i32) -> AppResult<LogFetchInfo> {
        trace!(
            "topic partition: {} read_records offset: {}, max_bytes: {}",
            self.topic_partition.id(),
            offset,
            max_bytes
        );
        if max_bytes <= 0 {
            return Ok(LogFetchInfo {
                records: MemoryRecords::empty(),
                log_start_offset: 0,
                log_end_offset: 0,
                position_info: PositionInfo::default(),
            });
        }
        let local_replica_id = global_config().general.id;

        let replica = self
            .assigned_replicas
            .get(&local_replica_id)
            .ok_or_else(|| AppError::InvalidValue("replica", local_replica_id.to_string()))?;

        replica
            .log
            .read_records(&self.topic_partition, offset, max_bytes)
            .await
    }

    pub fn create_replica(&self, broker_id: i32, replica: QueueReplica) {
        self.assigned_replicas.insert(broker_id, Arc::new(replica));
    }

    pub async fn get_leo_info(&self) -> AppResult<PositionInfo> {
        let local_replica_id = global_config().general.id;
        let replica = self
            .assigned_replicas
            .get(&local_replica_id)
            .ok_or_else(|| AppError::InvalidValue("replica", local_replica_id.to_string()))?;

        let log_clone = replica.log.clone();
        drop(replica);

        log_clone.get_leo_info().await
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

impl Display for TopicPartition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

impl TopicPartition {
    pub fn new(topic: String, partition: i32) -> Self {
        Self { topic, partition }
    }

    pub fn id(&self) -> String {
        format!("{}-{}", self.topic, self.partition)
    }

    pub fn from_string(str_name: Cow<str>) -> AppResult<Self> {
        let (topic, partition) = str_name
            .rsplit_once('-')
            .ok_or_else(|| AppError::InvalidValue("topic partition name", str_name.to_string()))?;

        let partition = partition
            .parse()
            .map_err(|_| AppError::InvalidValue("topic partition id", partition.to_string()))?;

        Ok(Self::new(topic.to_string(), partition))
    }
    pub fn journal_partition_dir(&self) -> String {
        format!("{}/{}", global_config().log.journal_base_dir, self)
    }
    pub fn queue_partition_dir(&self) -> String {
        format!("{}/{}", global_config().log.queue_base_dir, self)
    }
    pub fn protocol_size(&self) -> u32 {
        4 + self.id().as_bytes().len() as u32
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartitionMsgData {
    pub partition: i32,
    pub message_set: MemoryRecords,
}

impl PartitionMsgData {
    pub fn new(partition: i32, message_set: MemoryRecords) -> Self {
        Self {
            partition,
            message_set,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TopicData {
    pub topic_name: String,
    pub partition_data: Vec<PartitionMsgData>,
}

impl TopicData {
    pub fn new(topic_name: String, partition_data: Vec<PartitionMsgData>) -> Self {
        Self {
            topic_name,
            partition_data,
        }
    }
}
