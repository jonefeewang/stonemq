use std::collections::HashMap;

use crate::message::TopicPartition;

use super::errors::KafkaError;

#[derive(Debug)]
pub struct FetchOffsetsRequest {
    pub group_id: String,
    pub partitions: Option<Vec<TopicPartition>>,
}

#[derive(Debug)]
pub struct FetchOffsetsResponse {
    pub error_code: KafkaError,
    pub throttle_time_ms: i32,
    pub offsets: HashMap<TopicPartition, PartitionOffsetData>,
}
impl FetchOffsetsResponse {
    pub fn new(
        error_code: KafkaError,
        offsets: HashMap<TopicPartition, PartitionOffsetData>,
    ) -> Self {
        FetchOffsetsResponse {
            error_code,
            throttle_time_ms: 0,
            offsets,
        }
    }
}
// 在fetch offsets response 中使用
#[derive(Debug)]
pub struct PartitionOffsetData {
    pub partition_id: i32,
    pub offset: i64,
    pub metadata: Option<String>,
    pub error: KafkaError,
}
