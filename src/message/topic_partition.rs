use std::borrow::Cow;
use std::fmt::{Display, Formatter};

use dashmap::DashMap;

use crate::message::records::MemoryRecords;
use crate::{global_config, AppResult};

use super::replica::{JournalReplica, QueueReplica};

#[derive(Debug, Clone)]
pub struct Partition<R> {
    pub topic_partition: TopicPartition,
    pub assigned_replicas: DashMap<i32, R>,
}

pub type JournalPartition = Partition<JournalReplica>;
pub type QueuePartition = Partition<QueueReplica>;

#[derive(Debug, Clone)]
pub struct LogAppendInfo {
    pub base_offset: i64,
    pub log_append_time: i64,
}

impl<R: Replica> Partition<R> {
    pub fn new(topic_partition: TopicPartition) -> Self {
        Self {
            topic_partition,
            assigned_replicas: DashMap::new(),
        }
    }

    pub async fn append_record_to_leader(
        &self,
        record: MemoryRecords,
        queue_topic_partition: TopicPartition,
    ) -> AppResult<LogAppendInfo> {
        let local_replica_id = global_config().general.id;

        let replica = self
            .assigned_replicas
            .get(&local_replica_id)
            .ok_or_else(|| InvalidValue("replica", local_replica_id.to_string()))?;

        replica
            .log()
            .append_records(queue_topic_partition, record)
            .await
    }

    pub fn create_replica(&self, broker_id: i32, replica: R) {
        self.assigned_replicas.insert(broker_id, replica);
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
            .ok_or_else(|| InvalidValue("topic partition name", str_name.to_string()))?;

        let partition = partition
            .parse()
            .map_err(|_| InvalidValue("topic partition id", partition.to_string()))?;

        Ok(Self::new(topic.to_string(), partition))
    }
    pub fn journal_log_dir(&self) -> String {
        format!(
            "{}/{}",
            global_config().log.journal_base_dir,
            self
        )
    }
    pub fn protocol_size(&self) -> u32 {
        4+self.id().as_bytes().len() as u32
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartitionData {
    pub partition: i32,
    pub message_set: MemoryRecords,
}

impl PartitionData {
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
    pub partition_data: Vec<PartitionData>,
}

impl TopicData {
    pub fn new(topic_name: String, partition_data: Vec<PartitionData>) -> Self {
        Self {
            topic_name,
            partition_data,
        }
    }
}
