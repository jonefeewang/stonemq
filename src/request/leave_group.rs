use super::errors::{KafkaError, KafkaResult};

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
