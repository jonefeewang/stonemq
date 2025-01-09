use std::fmt::{Display, Formatter};
use std::sync::Arc;

use dashmap::DashMap;

use tracing::trace;

use crate::log::{LogAppendInfo, LogType, PositionInfo};
use crate::message::memory_records::MemoryRecords;
use crate::replica::{JournalReplica, QueueReplica};
use crate::{global_config, AppError, AppResult};

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
        queue_topic_partition: &TopicPartition,
    ) -> AppResult<LogAppendInfo> {
        let local_replica_id = global_config().general.id;

        let log = {
            let replica = self.assigned_replicas.get(&local_replica_id);
            if replica.is_none() {
                return Err(AppError::IllegalStateError(format!(
                    "replica: {}, topic partition: {}",
                    local_replica_id,
                    queue_topic_partition.id()
                )));
            }
            replica.unwrap().log.clone()
        };

        log.append_records(record, queue_topic_partition).await
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

        let replica = {
            let replica = self.assigned_replicas.get(&local_replica_id).unwrap();
            replica.clone()
        };

        replica
            .log
            .read_records(&self.topic_partition, offset, max_bytes)
            .await
    }

    pub fn create_empty_fetch_info(&self) -> LogFetchInfo {
        let local_replica_id = global_config().general.id;
        let replica = {
            let replica = self.assigned_replicas.get(&local_replica_id).unwrap();
            replica.clone()
        };

        replica.log.create_empty_fetch_info()
    }

    pub fn create_replica(&self, broker_id: i32, replica: QueueReplica) {
        self.assigned_replicas.insert(broker_id, Arc::new(replica));
    }

    pub fn get_leo_info(&self) -> AppResult<PositionInfo> {
        let local_replica_id = global_config().general.id;
        let replica = {
            let replica = self
                .assigned_replicas
                .get(&local_replica_id)
                .ok_or_else(|| {
                    AppError::IllegalStateError(format!(
                        "replica: {}, topic partition: {}",
                        local_replica_id,
                        self.topic_partition.id()
                    ))
                })?;
            replica.clone()
        };

        replica.log.get_leo_info()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TopicPartition {
    topic: String,
    partition: i32,
    log_type: LogType,
}

impl Display for TopicPartition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.topic, self.partition)
    }
}

impl TopicPartition {
    fn parse_topic_partition(tp_str: &str) -> Option<(String, i32)> {
        let last_hyphen_idx = tp_str.rfind('-')?;
        let (topic, partition_str) = tp_str.split_at(last_hyphen_idx);
        let partition = partition_str[1..].parse::<i32>().ok()?;
        Some((topic.to_string(), partition))
    }
    pub fn from_str(tp_str: &str, log_type: LogType) -> AppResult<Self> {
        let (topic, partition) = Self::parse_topic_partition(tp_str).ok_or_else(|| {
            AppError::InvalidValue(format!("invalid topic partition  name: {}", tp_str))
        })?;
        Ok(Self {
            topic: topic.to_string(),
            partition,
            log_type,
        })
    }
    pub fn new(topic: impl Into<String>, partition: i32, log_type: LogType) -> Self {
        Self {
            topic: topic.into(),
            partition,
            log_type,
        }
    }

    pub fn new_journal(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
            log_type: LogType::Journal,
        }
    }

    pub fn new_queue(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
            log_type: LogType::Queue,
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }
    pub fn log_type(&self) -> LogType {
        self.log_type
    }

    pub fn is_journal(&self) -> bool {
        self.log_type == LogType::Journal
    }

    pub fn is_queue(&self) -> bool {
        self.log_type == LogType::Queue
    }

    pub fn id(&self) -> String {
        format!("{}-{}", self.topic, self.partition)
    }

    pub fn partition_dir(&self) -> String {
        if self.is_journal() {
            format!("{}/{}", global_config().log.journal_base_dir, self.id())
        } else {
            format!("{}/{}", global_config().log.queue_base_dir, self.id())
        }
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
