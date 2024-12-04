use std::collections::HashMap;

use tracing::debug;

use crate::message::TopicPartition;

use crate::request::errors::KafkaError;
use crate::request::request_context::RequestContext;

use crate::request::api::ApiHandler;

pub struct FetchOffsetsRequestHandler;
impl ApiHandler for FetchOffsetsRequestHandler {
    type Request = FetchOffsetsRequest;
    type Response = FetchOffsetsResponse;

    async fn handle_request(
        &self,
        request: FetchOffsetsRequest,
        context: &RequestContext,
    ) -> FetchOffsetsResponse {
        let result = context
            .group_coordinator
            .clone()
            .handle_fetch_offsets(&request.group_id, request.partitions);
        debug!("fetch offsets result: {:?}", result);
        if let Ok(offsets) = result {
            FetchOffsetsResponse::new(KafkaError::None, offsets)
        } else {
            FetchOffsetsResponse::new(result.err().unwrap(), HashMap::new())
        }
    }
}

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
