use tracing::debug;

use crate::{
    request::kafka_errors::{ErrorCode, KafkaError},
    request::RequestContext,
};

use super::ApiHandler;

pub struct HeartbeatRequestHandler;
impl ApiHandler for HeartbeatRequestHandler {
    type Request = HeartbeatRequest;
    type Response = HeartbeatResponse;

    async fn handle_request(
        &self,
        request: HeartbeatRequest,
        context: &RequestContext,
    ) -> HeartbeatResponse {
        debug!("received heartbeat request");
        let result = context
            .group_coordinator
            .clone()
            .handle_heartbeat(&request.group_id, &request.member_id, request.generation_id)
            .await;
        match result {
            Ok(_) => HeartbeatResponse::new(KafkaError::None, 0),
            Err(e) => HeartbeatResponse::new(e, 0),
        }
    }
}

#[derive(Debug)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub member_id: String,
    pub generation_id: i32,
}

#[derive(Debug)]
pub struct HeartbeatResponse {
    /**
     * Possible error codes:
     *
     * GROUP_COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR (16)
     * ILLEGAL_GENERATION (22)
     * UNKNOWN_MEMBER_ID (25)
     * REBALANCE_IN_PROGRESS (27)
     * GROUP_AUTHORIZATION_FAILED (30)
     */
    pub error_code: i16,
    pub throttle_time_ms: i32,
}
impl HeartbeatResponse {
    pub fn new(error: KafkaError, throttle_time_ms: i32) -> Self {
        let error_code = ErrorCode::from(&error);
        HeartbeatResponse {
            error_code: error_code as i16,
            throttle_time_ms,
        }
    }
}
