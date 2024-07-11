use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::hash::Hash;

use dashmap::DashMap;

use crate::{AppResult, BROKER_CONFIG};
use crate::AppError::InvalidValue;
use crate::log::Log;
use crate::message::MemoryRecords;
use crate::replica::Replica;

#[derive(Debug)]
pub struct Partition<T: Log> {
    pub topic_partition: TopicPartition,
    pub assigned_replicas: DashMap<i32, Replica<T>>,
}

pub struct LogAppendInfo {
    pub base_offset: i64,
    pub log_append_time: i64,
}
impl<T: Log> Partition<T> {
    pub fn new(topic_partition: TopicPartition) -> Self {
        Partition {
            topic_partition,
            assigned_replicas: DashMap::new(),
        }
    }
    ///
    /// Append a record to the leader replica of this partition
    /// # Arguments
    /// * `record` - The record to append
    /// * `queue_topic_partition` - The topic partition of the queue log
    /// # Return
    pub async fn append_record_to_leader(
        &self,
        record: MemoryRecords,
        queue_topic_partition: TopicPartition,
    ) -> AppResult<LogAppendInfo> {
        let local_replica_id = &BROKER_CONFIG.get().unwrap().general.id;
        let replica = self
            .assigned_replicas
            .get(local_replica_id)
            .ok_or(InvalidValue("replica", local_replica_id.to_string()))?;

        replica
            .log
            .append_records((queue_topic_partition, record))
            .await
    }
    pub fn create_replica(&mut self, broker_id: i32, replica: Replica<T>) {
        self.assigned_replicas.entry(broker_id).or_insert(replica);
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
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
    pub fn id(&self) -> String {
        format!("{}-{}", self.topic, self.partition)
    }
    pub fn from_string(str_name: Cow<str>) -> AppResult<TopicPartition> {
        let index = str_name
            .rfind('-')
            .ok_or(InvalidValue("topic partition name", str_name.to_string()))?;
        let topic_partition = TopicPartition {
            topic: str_name.as_ref()[..index].to_string(),
            partition: str_name.as_ref()[index + 1..]
                .parse()
                .map_err(|_| InvalidValue("topic partition id", str_name.as_ref().to_string()))?,
        };
        Ok(topic_partition)
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartitionData {
    pub partition: i32,
    pub message_set: MemoryRecords,
}
impl PartitionData {
    pub fn new(partition: i32, message_set: MemoryRecords) -> PartitionData {
        PartitionData {
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
    pub fn new(topic_name: String, partition_data: Vec<PartitionData>) -> TopicData {
        TopicData {
            topic_name,
            partition_data,
        }
    }
}
