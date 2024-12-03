use super::errors::{ErrorCode, KafkaError};

#[derive(Debug)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub member_id: String,
    pub generation_id: i32,
}
impl HeartbeatRequest {
    pub const GROUP_ID_KEY_NAME: &'static str = "group_id";
    pub const GROUP_GENERATION_ID_KEY_NAME: &'static str = "group_generation_id";
    pub const MEMBER_ID_KEY_NAME: &'static str = "member_id";
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
    pub const ERROR_CODE_KEY_NAME: &'static str = "error_code";
    pub fn new(error: KafkaError, throttle_time_ms: i32) -> Self {
        let error_code = ErrorCode::from(&error);
        HeartbeatResponse {
            error_code: error_code as i16,
            throttle_time_ms,
        }
    }
}
