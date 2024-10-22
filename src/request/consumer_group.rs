use std::collections::BTreeMap;

use crate::service::Node;

pub struct FindCoordinatorRequest {
    pub group_id: String,
    pub coordinator_key: String,
    pub coordinator_type: String,
}

impl FindCoordinatorRequest {
    pub const GROUP_ID_KEY_NAME: &'static str = "group_id";
    pub const COORDINATOR_KEY_KEY_NAME: &'static str = "coordinator_key";
    pub const COORDINATOR_TYPE_KEY_NAME: &'static str = "coordinator_type";
}

pub struct FindCoordinatorResponse {
    pub throttle_time_ms: i32,
    pub error_message: String,
    pub error: i32,
    pub node: Node,
}
impl FindCoordinatorResponse {
    pub const ERROR_CODE_KEY_NAME: &'static str = "error_code";
    pub const ERROR_MESSAGE_KEY_NAME: &'static str = "error_message";
    pub const COORDINATOR_KEY_NAME: &'static str = "coordinator";

    // 可能的错误代码：
    //
    // COORDINATOR_NOT_AVAILABLE (15)
    // NOT_COORDINATOR (16)
    // GROUP_AUTHORIZATION_FAILED (30)

    // 协调器级别的字段名
    pub const NODE_ID_KEY_NAME: &'static str = "node_id";
    pub const HOST_KEY_NAME: &'static str = "host";
    pub const PORT_KEY_NAME: &'static str = "port";
}

impl From<Node> for FindCoordinatorResponse {
    fn from(node: Node) -> Self {
        FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_message: "".to_string(),
            error: 0,
            node,
        }
    }
}

pub struct JoinGroupRequest {
    pub group_id: String,
    pub session_timeout: i32,
    pub rebalance_timeout: i32,
    pub member_id: String,
    pub protocol_type: String,
    pub group_protocols: Vec<ProtocolMetadata>,
}

impl JoinGroupRequest {
    pub const GROUP_ID_KEY_NAME: &'static str = "group_id";
    pub const SESSION_TIMEOUT_KEY_NAME: &'static str = "session_timeout";
    pub const REBALANCE_TIMEOUT_KEY_NAME: &'static str = "rebalance_timeout";
    pub const MEMBER_ID_KEY_NAME: &'static str = "member_id";
    pub const PROTOCOL_TYPE_KEY_NAME: &'static str = "protocol_type";
    pub const GROUP_PROTOCOLS_KEY_NAME: &'static str = "group_protocols";
    pub const PROTOCOL_NAME_KEY_NAME: &'static str = "protocol_name";
    pub const PROTOCOL_METADATA_KEY_NAME: &'static str = "protocol_metadata";

    pub const UNKNOWN_MEMBER_ID: &'static str = "";
}

pub struct ProtocolMetadata {
    pub name: String,
    pub metadata: Vec<u8>,
}

impl ProtocolMetadata {
    pub fn new(name: String, metadata: Vec<u8>) -> Self {
        ProtocolMetadata { name, metadata }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn metadata(&self) -> &[u8] {
        &self.metadata
    }
}

pub struct JoinGroupResponse {
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
    pub members: BTreeMap<String, Vec<u8>>,
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
        members: BTreeMap<String, Vec<u8>>,
    ) -> Self {
        JoinGroupResponse {
            error_code: error,
            generation_id,
            group_protocol,
            member_id,
            leader_id,
            members,
        }
    }
}
