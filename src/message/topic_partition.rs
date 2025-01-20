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

//! Topic and Partition Management Implementation
//!
//! This module implements the core functionality for managing topics and partitions
//! in the messaging system. It provides structures and operations for both journal
//! and queue partitions, along with their associated replicas.
//!
//! # Architecture
//!
//! The system supports two types of partitions:
//! - Journal partitions: For write-ahead logging
//! - Queue partitions: For message queuing
//!
//! # Features
//!
//! - Concurrent replica management
//! - Partition-level operations
//! - Topic identification and routing
//! - Directory structure management
//! - Protocol support

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use dashmap::DashMap;
use tracing::trace;

use crate::log::{LogAppendInfo, LogType, PositionInfo};
use crate::message::memory_records::MemoryRecords;
use crate::replica::{JournalReplica, QueueReplica};
use crate::{global_config, AppError, AppResult};

use super::LogFetchInfo;

/// Journal partition implementation.
///
/// Manages write-ahead logging partitions and their replicas.
///
/// # Fields
///
/// * `_topic_partition` - Topic partition information
/// * `assigned_replicas` - Map of broker IDs to replicas
#[derive(Debug)]
pub struct JournalPartition {
    pub _topic_partition: TopicPartition,
    pub assigned_replicas: DashMap<i32, Arc<JournalReplica>>,
}

/// Queue partition implementation.
///
/// Manages message queuing partitions and their replicas.
///
/// # Fields
///
/// * `topic_partition` - Topic partition information
/// * `assigned_replicas` - Map of broker IDs to replicas
#[derive(Debug)]
pub struct QueuePartition {
    pub topic_partition: TopicPartition,
    pub assigned_replicas: DashMap<i32, Arc<QueueReplica>>,
}

impl JournalPartition {
    /// Creates a new journal partition.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - Topic partition information
    pub fn new(topic_partition: TopicPartition) -> Self {
        Self {
            _topic_partition: topic_partition,
            assigned_replicas: DashMap::new(),
        }
    }

    /// Appends a record to the leader replica.
    ///
    /// # Arguments
    ///
    /// * `record` - Records to append
    /// * `queue_topic_partition` - Destination queue partition
    ///
    /// # Returns
    ///
    /// * `AppResult<LogAppendInfo>` - Information about the append operation
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

    /// Creates and assigns a new replica.
    ///
    /// # Arguments
    ///
    /// * `broker_id` - ID of the broker to assign the replica to
    /// * `replica` - The replica to assign
    pub fn create_replica(&self, broker_id: i32, replica: JournalReplica) {
        self.assigned_replicas.insert(broker_id, Arc::new(replica));
    }
}

impl QueuePartition {
    /// Creates a new queue partition.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - Topic partition information
    pub fn new(topic_partition: TopicPartition) -> Self {
        Self {
            topic_partition,
            assigned_replicas: DashMap::new(),
        }
    }

    /// Reads records from the partition.
    ///
    /// # Arguments
    ///
    /// * `offset` - Offset to start reading from
    /// * `max_bytes` - Maximum bytes to read
    ///
    /// # Returns
    ///
    /// * `AppResult<LogFetchInfo>` - Fetched records and metadata
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

    /// Creates an empty fetch info response.
    ///
    /// # Returns
    ///
    /// Empty LogFetchInfo instance
    pub fn create_empty_fetch_info(&self) -> LogFetchInfo {
        let local_replica_id = global_config().general.id;
        let replica = {
            let replica = self.assigned_replicas.get(&local_replica_id).unwrap();
            replica.clone()
        };

        replica.log.create_empty_fetch_info()
    }

    /// Creates and assigns a new replica.
    ///
    /// # Arguments
    ///
    /// * `broker_id` - ID of the broker to assign the replica to
    /// * `replica` - The replica to assign
    pub fn create_replica(&self, broker_id: i32, replica: QueueReplica) {
        self.assigned_replicas.insert(broker_id, Arc::new(replica));
    }

    /// Gets the Log End Offset (LEO) information.
    ///
    /// # Returns
    ///
    /// * `AppResult<PositionInfo>` - Position information for the LEO
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

/// Topic partition identification and management.
///
/// Represents a unique partition within a topic and its associated type (journal/queue).
///
/// # Fields
///
/// * `topic` - Name of the topic
/// * `partition` - Partition number
/// * `log_type` - Type of log (journal/queue)
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
    /// Parses a topic partition from a string.
    ///
    /// Format: "<topic>-<partition>"
    ///
    /// # Arguments
    ///
    /// * `tp_str` - String to parse
    ///
    /// # Returns
    ///
    /// * `Option<(String, i32)>` - Topic name and partition number if valid
    fn parse_topic_partition(tp_str: &str) -> Option<(String, i32)> {
        let last_hyphen_idx = tp_str.rfind('-')?;
        let (topic, partition_str) = tp_str.split_at(last_hyphen_idx);
        let partition = partition_str[1..].parse::<i32>().ok()?;
        Some((topic.to_string(), partition))
    }

    /// Creates a TopicPartition from a string representation.
    ///
    /// # Arguments
    ///
    /// * `tp_str` - String to parse
    /// * `log_type` - Type of log
    ///
    /// # Returns
    ///
    /// * `AppResult<Self>` - New TopicPartition instance
    pub fn from_str(tp_str: &str, log_type: LogType) -> AppResult<Self> {
        let (topic, partition) = Self::parse_topic_partition(tp_str).ok_or_else(|| {
            AppError::InvalidValue(format!("invalid topic partition name: {}", tp_str))
        })?;
        Ok(Self {
            topic: topic.to_string(),
            partition,
            log_type,
        })
    }

    /// Creates a new TopicPartition instance.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name
    /// * `partition` - Partition number
    /// * `log_type` - Type of log
    pub fn new(topic: impl Into<String>, partition: i32, log_type: LogType) -> Self {
        Self {
            topic: topic.into(),
            partition,
            log_type,
        }
    }

    /// Creates a new journal partition.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name
    /// * `partition` - Partition number
    pub fn new_journal(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
            log_type: LogType::Journal,
        }
    }

    /// Creates a new queue partition.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name
    /// * `partition` - Partition number
    pub fn new_queue(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
            log_type: LogType::Queue,
        }
    }

    /// Gets the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Gets the partition number.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Gets the log type.
    pub fn log_type(&self) -> LogType {
        self.log_type
    }

    /// Checks if this is a journal partition.
    pub fn is_journal(&self) -> bool {
        self.log_type == LogType::Journal
    }

    /// Checks if this is a queue partition.
    pub fn is_queue(&self) -> bool {
        self.log_type == LogType::Queue
    }

    /// Gets the partition identifier string.
    pub fn id(&self) -> String {
        format!("{}-{}", self.topic, self.partition)
    }

    /// Gets the partition directory path.
    pub fn partition_dir(&self) -> String {
        if self.is_journal() {
            format!("{}/{}", global_config().log.journal_base_dir, self.id())
        } else {
            format!("{}/{}", global_config().log.queue_base_dir, self.id())
        }
    }

    /// Gets the size needed for protocol encoding.
    pub fn protocol_size(&self) -> u32 {
        4 + self.id().as_bytes().len() as u32
    }
}

/// Message data for a partition.
///
/// Contains the partition number and associated messages.
///
/// # Fields
///
/// * `partition` - Partition number
/// * `message_set` - Messages for this partition
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartitionMsgData {
    pub partition: i32,
    pub message_set: MemoryRecords,
}

impl PartitionMsgData {
    /// Creates a new PartitionMsgData instance.
    pub fn new(partition: i32, message_set: MemoryRecords) -> Self {
        Self {
            partition,
            message_set,
        }
    }
}

/// Message data for a topic.
///
/// Contains the topic name and message data for its partitions.
///
/// # Fields
///
/// * `topic_name` - Name of the topic
/// * `partition_data` - Message data for each partition
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TopicData {
    pub topic_name: String,
    pub partition_data: Vec<PartitionMsgData>,
}

impl TopicData {
    /// Creates a new TopicData instance.
    pub fn new(topic_name: String, partition_data: Vec<PartitionMsgData>) -> Self {
        Self {
            topic_name,
            partition_data,
        }
    }
}
