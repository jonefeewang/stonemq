use std::{
    collections::{BTreeMap, HashMap},
    io::Read,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{message::TopicPartition, service::Node, AppResult};

use super::errors::{ErrorCode, KafkaError, KafkaResult};

#[derive(Debug)]
pub struct FindCoordinatorRequest {
    pub coordinator_key: String,
    pub coordinator_type: i8,
}

impl FindCoordinatorRequest {
    pub const GROUP_ID_KEY_NAME: &'static str = "group_id";
    pub const COORDINATOR_KEY_KEY_NAME: &'static str = "coordinator_key";
    pub const COORDINATOR_TYPE_KEY_NAME: &'static str = "coordinator_type";
}
#[derive(Debug)]
pub struct FindCoordinatorResponse {
    pub throttle_time_ms: i32,
    pub error_message: Option<String>,
    pub error: i16,
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
            error_message: None,
            error: 0,
            node,
        }
    }
}

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

#[derive(Debug)]
pub struct SyncGroupRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_assignment: HashMap<String, BytesMut>,
}
impl SyncGroupRequest {
    pub const GROUP_ID_KEY_NAME: &'static str = "group_id";
    pub const GENERATION_ID_KEY_NAME: &'static str = "generation_id";
    pub const MEMBER_ID_KEY_NAME: &'static str = "member_id";
    pub const GROUP_ASSIGNMENT_KEY_NAME: &'static str = "group_assignment";
    pub const MEMBER_ASSIGNMENT_KEY_NAME: &'static str = "member_assignment";
    pub fn new(
        group_id: String,
        generation_id: i32,
        member_id: String,
        group_assignment: HashMap<String, BytesMut>,
    ) -> Self {
        SyncGroupRequest {
            group_id,
            generation_id,
            member_id,
            group_assignment,
        }
    }
}

#[derive(Debug)]
pub struct SyncGroupResponse {
    pub error_code: ErrorCode,
    pub throttle_time_ms: i32,
    pub member_assignment: Bytes,
}
impl SyncGroupResponse {
    pub const ERROR_CODE_KEY_NAME: &'static str = "error_code";
    pub const THROTTLE_TIME_MS_KEY_NAME: &'static str = "throttle_time_ms";
    pub const MEMBER_ASSIGNMENT_KEY_NAME: &'static str = "member_assignment";
    pub fn new(error_code: ErrorCode, throttle_time_ms: i32, member_assignment: Bytes) -> Self {
        SyncGroupResponse {
            error_code,
            throttle_time_ms,
            member_assignment,
        }
    }
}

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
#[derive(Debug)]
pub struct PartitionOffsetCommitData {
    pub partition_id: i32,
    pub offset: i64,
    pub metadata: Option<String>,
}
impl PartitionOffsetCommitData {
    pub fn serialize(&self) -> AppResult<Bytes> {
        let mut buf = BytesMut::new();
        buf.put_i32(self.partition_id);
        buf.put_i64(self.offset);
        if let Some(metadata) = &self.metadata {
            buf.put_i32(metadata.len() as i32);
            buf.put_slice(metadata.as_bytes());
        } else {
            buf.put_i32(-1);
        }
        Ok(buf.freeze())
    }
    pub fn deserialize(bytes: &[u8]) -> AppResult<Self> {
        let mut cursor = std::io::Cursor::new(bytes);
        let partition_id = cursor.get_i32();
        let offset = cursor.get_i64();
        let metadata_len = cursor.get_i32();
        let metadata = if metadata_len != -1 {
            let mut metadata = vec![0; metadata_len as usize];
            cursor.read_exact(&mut metadata)?;
            Some(String::from_utf8(metadata)?)
        } else {
            None
        };
        Ok(PartitionOffsetCommitData {
            partition_id,
            offset,
            metadata,
        })
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

#[derive(Debug)]
pub struct OffsetCommitRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub retention_time: i64,
    pub offset_data: HashMap<TopicPartition, PartitionOffsetCommitData>,
}

#[derive(Debug)]
pub struct OffsetCommitResponse {
    pub throttle_time_ms: i32,
    pub responses: HashMap<TopicPartition, Vec<(i32, KafkaError)>>,
}
impl OffsetCommitResponse {
    pub fn new(
        throttle_time_ms: i32,
        responses: HashMap<TopicPartition, Vec<(i32, KafkaError)>>,
    ) -> Self {
        OffsetCommitResponse {
            throttle_time_ms,
            responses,
        }
    }
}
