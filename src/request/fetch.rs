use std::collections::BTreeMap;

use crate::message::{MemoryRecords, TopicPartition};

pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
}

pub struct FetchRequest {
    pub replica_id: i32,
    pub max_wait: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: IsolationLevel,
    pub fetch_data: BTreeMap<TopicPartition, PartitionDataReq>,
}

impl FetchRequest {
    pub const CONSUMER_REPLICA_ID: i32 = -1;
    pub const REPLICA_ID_KEY_NAME: &'static str = "replica_id";
    pub const MAX_WAIT_KEY_NAME: &'static str = "max_wait_time";
    pub const MIN_BYTES_KEY_NAME: &'static str = "min_bytes";
    pub const ISOLATION_LEVEL_KEY_NAME: &'static str = "isolation_level";
    pub const TOPICS_KEY_NAME: &'static str = "topics";
    pub const MAX_BYTES_KEY_NAME: &'static str = "max_bytes";
    pub const TOPIC_KEY_NAME: &'static str = "topic";
    pub const PARTITIONS_KEY_NAME: &'static str = "partitions";
    pub const PARTITION_KEY_NAME: &'static str = "partition";
    pub const FETCH_OFFSET_KEY_NAME: &'static str = "fetch_offset";
    pub const LOG_START_OFFSET_KEY_NAME: &'static str = "log_start_offset";
    pub const DEFAULT_RESPONSE_MAX_BYTES: i32 = i32::MAX;
    pub const INVALID_LOG_START_OFFSET: i64 = -1;
}

pub struct PartitionDataReq {
    pub fetch_offset: i64,
    pub log_start_offset: i64,
    pub max_bytes: i32,
}

impl PartitionDataReq {
    pub fn new(fetch_offset: i64, log_start_offset: i64, max_bytes: i32) -> Self {
        PartitionDataReq {
            fetch_offset,
            log_start_offset,
            max_bytes,
        }
    }
}

impl std::fmt::Display for PartitionDataReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(offset={}, logStartOffset={}, maxBytes={})",
            self.fetch_offset, self.log_start_offset, self.max_bytes
        )
    }
}

pub struct TopicAndPartitionData<T> {
    pub topic: String,
    pub partitions: BTreeMap<i32, T>,
}

impl<T> TopicAndPartitionData<T> {
    pub fn new(topic: String) -> Self {
        TopicAndPartitionData {
            topic,
            partitions: BTreeMap::new(),
        }
    }

    pub fn batch_by_topic(data: &BTreeMap<TopicPartition, T>) -> Vec<TopicAndPartitionData<T>>
    where
        T: Clone,
    {
        let mut topics: Vec<TopicAndPartitionData<T>> = Vec::new();
        for (topic_partition, partition_data) in data {
            let topic = topic_partition.topic.clone();
            let partition = topic_partition.partition;

            if topics.is_empty() || topics.last().unwrap().topic != topic {
                topics.push(TopicAndPartitionData::new(topic.clone()));
            }

            if let Some(last_topic) = topics.last_mut() {
                last_topic
                    .partitions
                    .insert(partition, partition_data.clone());
            }
        }
        topics
    }
}

pub struct FetchResponse {
    /**
     * Possible error codes:
     *
     *  OFFSET_OUT_OF_RANGE (1)
     *  UNKNOWN_TOPIC_OR_PARTITION (3)
     *  NOT_LEADER_FOR_PARTITION (6)
     *  REPLICA_NOT_AVAILABLE (9)
     *  UNKNOWN (-1)
     */
    pub responses: BTreeMap<TopicPartition, PartitionDataRep>,
    pub throttle_time: i32,
}
impl FetchResponse {
    pub const RESPONSES_KEY_NAME: &'static str = "responses";

    // topic level field names
    pub const TOPIC_KEY_NAME: &'static str = "topic";
    pub const PARTITIONS_KEY_NAME: &'static str = "partition_responses";

    // partition level field names
    pub const PARTITION_HEADER_KEY_NAME: &'static str = "partition_header";
    pub const PARTITION_KEY_NAME: &'static str = "partition";
    pub const ERROR_CODE_KEY_NAME: &'static str = "error_code";
    pub const HIGH_WATERMARK_KEY_NAME: &'static str = "high_watermark";
    pub const LAST_STABLE_OFFSET_KEY_NAME: &'static str = "last_stable_offset";
    pub const LOG_START_OFFSET_KEY_NAME: &'static str = "log_start_offset";
    pub const ABORTED_TRANSACTIONS_KEY_NAME: &'static str = "aborted_transactions";
    pub const RECORD_SET_KEY_NAME: &'static str = "record_set";

    // aborted transaction field names
    pub const PRODUCER_ID_KEY_NAME: &'static str = "producer_id";
    pub const FIRST_OFFSET_KEY_NAME: &'static str = "first_offset";

    pub const DEFAULT_THROTTLE_TIME: i32 = 0;
    pub const INVALID_HIGHWATERMARK: i64 = -1;
    pub const INVALID_LAST_STABLE_OFFSET: i64 = -1;
    pub const INVALID_LOG_START_OFFSET: i64 = -1;
}

pub struct PartitionDataRep {
    pub error_code: i16,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    pub aborted_transactions: Option<Vec<AbortedTransaction>>,
    pub records: MemoryRecords,
}

pub struct AbortedTransaction {
    pub producer_id: i64,
    pub first_offset: i64,
}

impl From<BTreeMap<TopicPartition, (MemoryRecords, i64, i64)>> for FetchResponse {
    fn from(value: BTreeMap<TopicPartition, (MemoryRecords, i64, i64)>) -> Self {
        let mut responses = BTreeMap::new();
        for (topic_partition, (records, log_start_offset, log_end_offset)) in value {
            responses.insert(
                topic_partition,
                PartitionDataRep {
                    error_code: 0,
                    high_watermark: log_end_offset,
                    last_stable_offset: log_end_offset,
                    log_start_offset,
                    aborted_transactions: None,
                    records,
                },
            );
        }
        FetchResponse {
            responses,
            throttle_time: 0,
        }
    }
}
