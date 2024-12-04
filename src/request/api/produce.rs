use std::borrow::Cow;
use std::collections::BTreeMap;

use tracing::instrument;

use crate::message::{TopicData, TopicPartition};
use crate::protocol::Acks;
use crate::{AppError, AppResult};

use crate::request::RequestContext;

use super::ApiHandler;

#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub transactional_id: Option<String>,
    pub required_acks: Acks,
    pub timeout: i32,
    pub topic_data: Vec<TopicData>,
}

impl ProduceRequest {
    pub fn new(
        transactional_id: Option<String>,
        required_acks: Acks,
        timeout: i32,
        topic_data: Vec<TopicData>,
    ) -> ProduceRequest {
        ProduceRequest {
            transactional_id,
            required_acks,
            timeout,
            topic_data,
        }
    }

    pub fn validate(&self) -> AppResult<()> {
        if self.timeout < 0 {
            return Err(AppError::RequestError(Cow::Borrowed(
                "timeout must be >= 0",
            )));
        }
        Ok(())
    }
}
impl PartialEq for ProduceRequest {
    fn eq(&self, other: &Self) -> bool {
        self.transactional_id == other.transactional_id
            && self.required_acks == other.required_acks
            && self.timeout == other.timeout
            && self.topic_data == other.topic_data
    }
}
impl Eq for ProduceRequest {}
#[derive(Debug)]
pub struct PartitionResponse {
    pub partition: i32,
    pub error_code: i16,
    pub base_offset: i64,
    // The timestamp returned by broker after appending the messages.
    // If CreateTime is used for the topic, the timestamp will be -1.
    // If LogAppendTime is used for the topic, the timestamp will be
    // the broker local time when the messages are appended
    pub log_append_time: i64,
}
#[derive(Debug)]
pub struct ProduceResponse {
    pub responses: BTreeMap<TopicPartition, PartitionResponse>,
    pub throttle_time: Option<i32>,
}

pub struct ProduceRequestHandler;

impl ApiHandler for ProduceRequestHandler {
    type Request = ProduceRequest;
    type Response = ProduceResponse;

    #[instrument(skip(self, request, context))]
    async fn handle_request(
        &self,
        request: ProduceRequest,
        context: &RequestContext,
    ) -> ProduceResponse {
        let tp_response = context
            .replica_manager
            .append_records(request.topic_data)
            .await;

        ProduceResponse {
            responses: tp_response,
            throttle_time: None,
        }
    }
}
