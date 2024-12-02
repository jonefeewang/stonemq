use std::sync::Arc;

use once_cell::sync::Lazy;

use super::{consumer_protocol::TOPICS_KEY_NAME, ERROR_CODE_KEY_NAME, THROTTLE_TIME_KEY_NAME};
use crate::protocol::{
    array::ArrayType,
    primary_types::{NPBytes, NPString, PString, I16, I32, I64, I8},
    schema::Schema,
    types::DataType,
};
pub mod fetch;
pub mod find_coordinator;
pub mod heartbeat;
pub mod join_group;
pub mod leave_group;
pub mod offsets_commit_fetch;
pub mod sync_group;

pub const PARTITION_KEY_NAME: &str = "partition";
pub const PARTITIONS_KEY_NAME: &str = "partitions";
pub const TOPIC_KEY_NAME: &str = "topic";

pub const OFFSET_KEY_NAME: &str = "offset";
pub const MAX_BYTES_KEY_NAME: &str = "max_bytes";

pub const GROUP_ID_KEY_NAME: &str = "group_id";
pub const SESSION_TIMEOUT_KEY_NAME: &str = "session_timeout";
pub const REBALANCE_TIMEOUT_KEY_NAME: &str = "rebalance_timeout";
pub const MEMBER_ID_KEY_NAME: &str = "member_id";
pub const PROTOCOL_TYPE_KEY_NAME: &str = "protocol_type";
pub const GROUP_PROTOCOLS_KEY_NAME: &str = "group_protocols";
pub const PROTOCOL_NAME_KEY_NAME: &str = "protocol_name";
pub const PROTOCOL_METADATA_KEY_NAME: &str = "protocol_metadata";
pub const GENERATION_ID_KEY_NAME: &str = "generation_id";
pub const GROUP_PROTOCOL_KEY_NAME: &str = "group_protocol";
pub const LEADER_ID_KEY_NAME: &str = "leader_id";
pub const MEMBERS_KEY_NAME: &str = "members";
pub const MEMBER_METADATA_KEY_NAME: &str = "member_metadata";
pub const MEMBER_ASSIGNMENT_KEY_NAME: &str = "member_assignment";
pub const GROUP_ASSIGNMENT_KEY_NAME: &str = "group_assignment";

pub const PARTITION_RESPONSES_KEY_NAME: &str = "partition_responses";
pub const METADATA_KEY_NAME: &str = "metadata";

pub const GROUP_GENERATION_ID_KEY_NAME: &str = "group_generation_id";
pub const RESPONSES_KEY_NAME: &str = "responses";
pub const RETENTION_TIME_KEY_NAME: &str = "retention_time";
pub const UNKNOWN_MEMBER_ID: &str = "";

pub const COORDINATOR_KEY_KEY_NAME: &str = "coordinator_key";
pub const COORDINATOR_TYPE_KEY_NAME: &str = "coordinator_type";

pub const NODE_ID_KEY_NAME: &str = "node_id";
pub const NODE_HOST_KEY_NAME: &str = "host";
pub const NODE_PORT_KEY_NAME: &str = "port";

pub const ERROR_MESSAGE_KEY_NAME: &str = "error_message";
pub const COORDINATOR_KEY_NAME: &str = "coordinator";

pub static LEAVE_GROUP_REQUEST_V1_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, GROUP_ID_KEY_NAME, DataType::PString(PString::default())),
        (1, MEMBER_ID_KEY_NAME, DataType::NPBytes(NPBytes::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static LEAVE_GROUP_RESPONSE_V1_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, THROTTLE_TIME_KEY_NAME, DataType::I32(I32::default())),
        (1, ERROR_CODE_KEY_NAME, DataType::I16(I16::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_FETCH_REQUEST_PARTITION_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> =
        vec![(0, PARTITION_KEY_NAME, DataType::I32(I32::default()))];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_FETCH_REQUEST_TOPIC_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, TOPIC_KEY_NAME, DataType::PString(PString::default())),
        (
            1,
            PARTITIONS_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(
                    OFFSET_FETCH_REQUEST_PARTITION_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];

    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_FETCH_REQUEST_V3_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, GROUP_ID_KEY_NAME, DataType::PString(PString::default())),
        (
            1,
            TOPICS_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: true,
                p_type: Arc::new(DataType::Schema(
                    OFFSET_FETCH_REQUEST_TOPIC_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_FETCH_RESPONSE_PARTITION_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, PARTITION_KEY_NAME, DataType::I32(I32::default())),
        (1, OFFSET_KEY_NAME, DataType::I64(I64::default())),
        (
            2,
            METADATA_KEY_NAME,
            DataType::NPString(NPString::default()),
        ),
        (3, ERROR_CODE_KEY_NAME, DataType::I16(I16::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_FETCH_RESPONSE_TOPIC_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, TOPIC_KEY_NAME, DataType::PString(PString::default())),
        (
            1,
            PARTITION_RESPONSES_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(
                    OFFSET_FETCH_RESPONSE_PARTITION_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_FETCH_RESPONSE_V3_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, THROTTLE_TIME_KEY_NAME, DataType::I32(I32::default())),
        (
            1,
            RESPONSES_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(
                    OFFSET_FETCH_RESPONSE_TOPIC_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
        (2, ERROR_CODE_KEY_NAME, DataType::I16(I16::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_COMMIT_REQUEST_PARTITION_V2_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, PARTITION_KEY_NAME, DataType::I32(I32::default())),
        (1, OFFSET_KEY_NAME, DataType::I64(I64::default())),
        (
            2,
            METADATA_KEY_NAME,
            DataType::NPString(NPString::default()),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_COMMIT_REQUEST_TOPIC_V2_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, TOPIC_KEY_NAME, DataType::PString(PString::default())),
        (
            1,
            PARTITIONS_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(
                    OFFSET_COMMIT_REQUEST_PARTITION_V2_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_COMMIT_REQUEST_V3_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, GROUP_ID_KEY_NAME, DataType::PString(PString::default())),
        (
            1,
            GROUP_GENERATION_ID_KEY_NAME,
            DataType::I32(I32::default()),
        ),
        (2, MEMBER_ID_KEY_NAME, DataType::PString(PString::default())),
        (3, RETENTION_TIME_KEY_NAME, DataType::I64(I64::default())),
        (
            4,
            TOPICS_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(
                    OFFSET_COMMIT_REQUEST_TOPIC_V2_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_COMMIT_RESPONSE_PARTITION_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, PARTITION_KEY_NAME, DataType::I32(I32::default())),
        (1, ERROR_CODE_KEY_NAME, DataType::I16(I16::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_COMMIT_RESPONSE_TOPIC_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, TOPIC_KEY_NAME, DataType::PString(PString::default())),
        (
            1,
            PARTITION_RESPONSES_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(
                    OFFSET_COMMIT_RESPONSE_PARTITION_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_COMMIT_RESPONSE_V3_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, THROTTLE_TIME_KEY_NAME, DataType::I32(I32::default())),
        (
            1,
            RESPONSES_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(
                    OFFSET_COMMIT_RESPONSE_TOPIC_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FIND_COORDINATOR_REQUEST_V1_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (
            0,
            COORDINATOR_KEY_KEY_NAME,
            DataType::PString(PString::default()),
        ),
        (1, COORDINATOR_TYPE_KEY_NAME, DataType::I8(I8::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FIND_COORDINATOR_BROKER_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, NODE_ID_KEY_NAME, DataType::I32(I32::default())),
        (1, NODE_HOST_KEY_NAME, DataType::PString(PString::default())),
        (2, NODE_PORT_KEY_NAME, DataType::I32(I32::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FIND_COORDINATOR_RESPONSE_V1_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, THROTTLE_TIME_KEY_NAME, DataType::I32(I32::default())),
        (1, ERROR_CODE_KEY_NAME, DataType::I16(I16::default())),
        (
            2,
            ERROR_MESSAGE_KEY_NAME,
            DataType::NPString(NPString::default()),
        ),
        (
            3,
            COORDINATOR_KEY_NAME,
            DataType::Schema(FIND_COORDINATOR_BROKER_V0_SCHEMA.clone()),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
