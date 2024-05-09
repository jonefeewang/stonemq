use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::Hash;

use crate::AppError::InvalidValue;
use crate::AppResult;
use crate::message::MemoryRecords;
use crate::replica::Replica;

pub struct Partition {
    assigned_replicas: HashMap<i32, Replica>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
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
    pub fn string_id(&self) -> String {
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
