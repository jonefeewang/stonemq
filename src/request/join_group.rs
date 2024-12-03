use std::collections::BTreeMap;

use bytes::{Bytes, BytesMut};

#[derive(Debug)]
pub struct ProtocolMetadata {
    // assignor.name
    pub name: String,
    // consumer protocol serialized subscription struct
    pub metadata: BytesMut,
}
impl PartialEq for ProtocolMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.metadata == other.metadata
    }
}

#[derive(Debug)]
pub struct JoinGroupRequest {
    pub client_id: String,
    pub client_host: String,
    pub group_id: String,
    pub session_timeout: i32,
    pub rebalance_timeout: i32,
    pub member_id: String,
    pub protocol_type: String,
    pub group_protocols: Vec<ProtocolMetadata>,
}

impl JoinGroupRequest {}

#[derive(Debug)]
pub struct JoinGroupResponse {
    pub throttle_time: Option<i32>,
    // 可能的错误代码：
    // COORDINATOR_LOAD_IN_PROGRESS (14)
    // GROUP_COORDINATOR_NOT_AVAILABLE (15)
    // NOT_COORDINATOR (16)
    // INCONSISTENT_GROUP_PROTOCOL (23)
    // UNKNOWN_MEMBER_ID (25)
    // INVALID_SESSION_TIMEOUT (26)
    // GROUP_AUTHORIZATION_FAILED (30)
    pub error_code: i16,
    pub generation_id: i32,
    pub group_protocol: String,
    pub member_id: String,
    pub leader_id: String,
    pub members: BTreeMap<String, Bytes>,
}

impl JoinGroupResponse {
    pub const ERROR_CODE_KEY_NAME: &'static str = "error_code";
    pub const GENERATION_ID_KEY_NAME: &'static str = "generation_id";
    pub const GROUP_PROTOCOL_KEY_NAME: &'static str = "group_protocol";
    pub const LEADER_ID_KEY_NAME: &'static str = "leader_id";
    pub const MEMBER_ID_KEY_NAME: &'static str = "member_id";
    pub const MEMBERS_KEY_NAME: &'static str = "members";
    pub const MEMBER_METADATA_KEY_NAME: &'static str = "member_metadata";

    pub const UNKNOWN_PROTOCOL: &'static str = "";
    pub const UNKNOWN_GENERATION_ID: i32 = -1;
    pub const UNKNOWN_MEMBER_ID: &'static str = "";

    pub fn new(
        error: i16,
        generation_id: i32,
        group_protocol: String,
        member_id: String,
        leader_id: String,
        members: BTreeMap<String, Bytes>,
    ) -> Self {
        JoinGroupResponse {
            throttle_time: None,
            error_code: error,
            generation_id,
            group_protocol,
            member_id,
            leader_id,
            members,
        }
    }
}
