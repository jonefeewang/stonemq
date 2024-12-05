use crate::{
    request::kafka_errors::{KafkaError, KafkaResult},
    request::RequestContext,
};

use super::ApiHandler;

pub struct LeaveGroupRequestHandler;
impl ApiHandler for LeaveGroupRequestHandler {
    type Request = LeaveGroupRequest;
    type Response = LeaveGroupResponse;

    async fn handle_request(
        &self,
        request: LeaveGroupRequest,
        context: &RequestContext,
    ) -> LeaveGroupResponse {
        let response_result = context
            .group_coordinator
            .clone()
            .handle_group_leave(&request.group_id, &request.member_id)
            .await;
        LeaveGroupResponse::new(response_result)
    }
}

#[derive(Debug)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    pub member_id: String,
}

#[derive(Debug)]
pub struct LeaveGroupResponse {
    pub error: KafkaError,
    pub throttle_time_ms: i32,
}
impl LeaveGroupResponse {
    pub fn new(result: KafkaResult<()>) -> Self {
        match result {
            Ok(_) => LeaveGroupResponse {
                error: KafkaError::None,
                throttle_time_ms: 0,
            },
            Err(e) => LeaveGroupResponse {
                error: e,
                throttle_time_ms: 0,
            },
        }
    }
}
