use std::clone::Clone;
use std::convert::TryFrom;
use std::sync::Arc;

use api_schemas::consumer_groups::fetch::{FETCH_REQUEST_V5_SCHEMA, FETCH_RESPONSE_V5_SCHEMA};
use api_schemas::consumer_groups::heartbeat::{
    HEARTBEAT_REQUEST_V1_SCHEMA, HEARTBEAT_RESPONSE_V1_SCHEMA,
};
use api_schemas::consumer_groups::join_group::{
    JOIN_GROUP_REQUEST_V2_SCHEMA, JOIN_GROUP_RESPONSE_V2_SCHEMA,
};
use api_schemas::consumer_groups::sync_group::{
    SYNC_GROUP_REQUEST_V1_SCHEMA, SYNC_GROUP_RESPONSE_V1_SCHEMA,
};
use api_schemas::consumer_groups::{
    FIND_COORDINATOR_REQUEST_V1_SCHEMA, FIND_COORDINATOR_RESPONSE_V1_SCHEMA,
    LEAVE_GROUP_RESPONSE_V1_SCHEMA, OFFSET_COMMIT_REQUEST_V3_SCHEMA,
    OFFSET_COMMIT_RESPONSE_V3_SCHEMA, OFFSET_FETCH_REQUEST_V3_SCHEMA,
    OFFSET_FETCH_RESPONSE_V3_SCHEMA,
};
use bytes::BytesMut;
use tokio::io::AsyncWriteExt;

use crate::protocol::api_schemas::metadata_reps::{
    METADATA_RESPONSE_V0, METADATA_RESPONSE_V1, METADATA_RESPONSE_V2, METADATA_RESPONSE_V3,
};
use crate::protocol::api_schemas::metadata_req::{
    METADATA_REQUEST_V0, METADATA_REQUEST_V1, METADATA_REQUEST_V4,
};
use crate::protocol::api_schemas::produce_reps::{
    PRODUCE_RESPONSE_V0, PRODUCE_RESPONSE_V1, PRODUCE_RESPONSE_V2,
};
use crate::protocol::api_schemas::produce_req::{
    PRODUCE_REQUEST_SCHEMA_V0, PRODUCE_REQUEST_SCHEMA_V3,
};
use crate::protocol::api_schemas::{
    API_VERSIONS_REQUEST_V0, API_VERSIONS_RESPONSE_V0, API_VERSIONS_RESPONSE_V1,
};
use crate::protocol::schema::Schema;
use crate::AppError::InvalidValue;
use crate::{AppError, AppResult};

pub mod api_schemas;
mod array;
mod field;
mod primary_types;
mod record;
mod schema;
mod types;
mod value_set;

pub trait ProtocolCodec<T> {
    ///
    /// 为了防止数据复制clone，这里直接消耗掉self，调用完成之后，self就无法再使用了
    async fn write_to<W>(
        self,
        writer: &mut W,
        api_version: &ApiVersion,
        correlation_id: i32,
    ) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin + Send;
    fn read_from(buffer: &mut BytesMut, api_version: &ApiVersion) -> AppResult<T>;

    fn fetch_request_schema_for_api(api_version: &ApiVersion, api_key: &ApiKey) -> Arc<Schema> {
        match api_key {
            ApiKey::Produce => match api_version {
                ApiVersion::V0 | ApiVersion::V1 | ApiVersion::V2 => {
                    Arc::clone(&PRODUCE_REQUEST_SCHEMA_V0)
                }
                ApiVersion::V3 => Arc::clone(&PRODUCE_REQUEST_SCHEMA_V3),

                ApiVersion::V4 | ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::Fetch => match api_version {
                ApiVersion::V0
                | ApiVersion::V1
                | ApiVersion::V2
                | ApiVersion::V3
                | ApiVersion::V4 => {
                    // too old, not support
                    todo!()
                }
                ApiVersion::V5 => Arc::clone(&FETCH_REQUEST_V5_SCHEMA),
            },

            ApiKey::Metadata => match api_version {
                ApiVersion::V0 => Arc::clone(&METADATA_REQUEST_V0),
                ApiVersion::V1 | ApiVersion::V2 | ApiVersion::V3 => {
                    Arc::clone(&METADATA_REQUEST_V1)
                }
                ApiVersion::V4 => Arc::clone(&METADATA_REQUEST_V4),
                ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::ApiVersion => match api_version {
                ApiVersion::V0 | ApiVersion::V1 => Arc::clone(&API_VERSIONS_REQUEST_V0),
                ApiVersion::V2 | ApiVersion::V3 | ApiVersion::V4 | ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::FindCoordinator => match api_version {
                ApiVersion::V0 => {
                    // too old, not support
                    todo!()
                }
                ApiVersion::V1 => Arc::clone(&FIND_COORDINATOR_REQUEST_V1_SCHEMA),
                ApiVersion::V2 | ApiVersion::V3 | ApiVersion::V4 => {
                    // not exist
                    todo!()
                }
                ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::JoinGroup => match api_version {
                ApiVersion::V1 | ApiVersion::V2 => Arc::clone(&JOIN_GROUP_REQUEST_V2_SCHEMA),
                ApiVersion::V0 => {
                    // too old, not support
                    todo!()
                }
                ApiVersion::V3 | ApiVersion::V4 => {
                    // not exist
                    todo!()
                }
                ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::SyncGroup => match api_version {
                ApiVersion::V0 | ApiVersion::V1 => Arc::clone(&SYNC_GROUP_REQUEST_V1_SCHEMA),

                ApiVersion::V2 | ApiVersion::V3 | ApiVersion::V4 => {
                    // not exist
                    todo!()
                }
                ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::LeaveGroup => match api_version {
                ApiVersion::V0 | ApiVersion::V1 => Arc::clone(&LEAVE_GROUP_RESPONSE_V1_SCHEMA),
                ApiVersion::V2 | ApiVersion::V3 | ApiVersion::V4 => {
                    // not exist
                    todo!()
                }
                ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::Heartbeat => match api_version {
                ApiVersion::V0 => {
                    // too old, not support
                    todo!()
                }
                ApiVersion::V1 => Arc::clone(&HEARTBEAT_REQUEST_V1_SCHEMA),
                ApiVersion::V2 | ApiVersion::V3 | ApiVersion::V4 => {
                    // not exist
                    todo!()
                }
                ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::OffsetCommit => match api_version {
                ApiVersion::V0 | ApiVersion::V1 => {
                    // too old, not support
                    todo!()
                }
                ApiVersion::V2 | ApiVersion::V3 => Arc::clone(&OFFSET_COMMIT_REQUEST_V3_SCHEMA),
                ApiVersion::V4 => {
                    // not exist
                    todo!()
                }
                ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::OffsetFetch => match api_version {
                ApiVersion::V0 | ApiVersion::V1 => {
                    // too old, not support
                    todo!()
                }
                ApiVersion::V3 | ApiVersion::V2 => Arc::clone(&OFFSET_FETCH_REQUEST_V3_SCHEMA),
                ApiVersion::V4 => {
                    // not exist
                    todo!()
                }
                ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
        }
    }
    fn fetch_response_schema_for_api(api_version: &ApiVersion, api_key: &ApiKey) -> Arc<Schema> {
        match api_key {
            ApiKey::Produce => match api_version {
                ApiVersion::V0 => Arc::clone(&PRODUCE_RESPONSE_V0),
                ApiVersion::V1 => Arc::clone(&PRODUCE_RESPONSE_V1),
                ApiVersion::V2 | ApiVersion::V3 | ApiVersion::V4 => {
                    Arc::clone(&PRODUCE_RESPONSE_V2)
                }
                ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::Fetch => match api_version {
                ApiVersion::V0
                | ApiVersion::V1
                | ApiVersion::V2
                | ApiVersion::V3
                | ApiVersion::V4 => {
                    // too old, not support
                    todo!()
                }
                ApiVersion::V5 => Arc::clone(&FETCH_RESPONSE_V5_SCHEMA),
            },
            ApiKey::Metadata => match api_version {
                ApiVersion::V0 => Arc::clone(&METADATA_RESPONSE_V0),
                ApiVersion::V1 => Arc::clone(&METADATA_RESPONSE_V1),
                ApiVersion::V2 => Arc::clone(&METADATA_RESPONSE_V2),
                ApiVersion::V3 | ApiVersion::V4 => Arc::clone(&METADATA_RESPONSE_V3),
                ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::ApiVersion => match api_version {
                ApiVersion::V0 => Arc::clone(&API_VERSIONS_RESPONSE_V0),
                ApiVersion::V1 => Arc::clone(&API_VERSIONS_RESPONSE_V1),
                _ => todo!(),
            },
            ApiKey::JoinGroup => match api_version {
                ApiVersion::V0 | ApiVersion::V1 => {
                    // too old, not support
                    todo!()
                }
                ApiVersion::V3 | ApiVersion::V4 | ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
                ApiVersion::V2 => Arc::clone(&JOIN_GROUP_RESPONSE_V2_SCHEMA),
            },
            ApiKey::SyncGroup => match api_version {
                ApiVersion::V1 => Arc::clone(&SYNC_GROUP_RESPONSE_V1_SCHEMA),
                ApiVersion::V0 => {
                    // too old, not support
                    todo!()
                }
                ApiVersion::V2 | ApiVersion::V3 | ApiVersion::V4 | ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::Heartbeat => match api_version {
                ApiVersion::V0 => {
                    // too old, not support
                    todo!()
                }
                ApiVersion::V1 => Arc::clone(&HEARTBEAT_RESPONSE_V1_SCHEMA),
                ApiVersion::V2 | ApiVersion::V3 | ApiVersion::V4 | ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::LeaveGroup => match api_version {
                ApiVersion::V0 | ApiVersion::V1 => Arc::clone(&LEAVE_GROUP_RESPONSE_V1_SCHEMA),
                ApiVersion::V2 | ApiVersion::V3 | ApiVersion::V4 | ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::OffsetFetch => match api_version {
                ApiVersion::V0 | ApiVersion::V1 | ApiVersion::V2 => {
                    // too old, not support
                    todo!()
                }
                ApiVersion::V3 => Arc::clone(&OFFSET_FETCH_RESPONSE_V3_SCHEMA),
                ApiVersion::V4 | ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::OffsetCommit => match api_version {
                ApiVersion::V0 | ApiVersion::V1 | ApiVersion::V2 => {
                    // too old, not support
                    todo!()
                }
                ApiVersion::V3 => Arc::clone(&OFFSET_COMMIT_RESPONSE_V3_SCHEMA),
                ApiVersion::V4 | ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
            ApiKey::FindCoordinator => match api_version {
                ApiVersion::V0 => {
                    // too old, not support
                    todo!()
                }
                ApiVersion::V1 => Arc::clone(&FIND_COORDINATOR_RESPONSE_V1_SCHEMA),
                ApiVersion::V2 | ApiVersion::V3 | ApiVersion::V4 | ApiVersion::V5 => {
                    // not exist
                    todo!()
                }
            },
        }
    }
}

/// Represents the different API keys used in the protocol.
///
/// The `ApiKey` enum is used to identify the type of request being made in the
/// protocol. It has three variants:
///
/// - `Produce`: Represents a request to produce data.
/// - `Metadata`: Represents a request for metadata about the system.
/// - `ApiVersion`: Represents a request for the API version information.
///
/// This enum is used throughout the protocol implementation to handle the
/// different types of requests and responses.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum ApiKey {
    Produce,
    Fetch,
    Metadata,
    ApiVersion,
    JoinGroup,
    SyncGroup,
    LeaveGroup,
    Heartbeat,
    OffsetCommit,
    OffsetFetch,
    FindCoordinator,
}
impl TryFrom<i16> for ApiKey {
    type Error = AppError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ApiKey::Produce),
            1 => Ok(ApiKey::Fetch),
            3 => Ok(ApiKey::Metadata),
            8 => Ok(ApiKey::OffsetCommit),
            9 => Ok(ApiKey::OffsetFetch),
            10 => Ok(ApiKey::FindCoordinator),
            11 => Ok(ApiKey::JoinGroup),
            12 => Ok(ApiKey::Heartbeat),
            13 => Ok(ApiKey::LeaveGroup),
            14 => Ok(ApiKey::SyncGroup),
            18 => Ok(ApiKey::ApiVersion),
            invalid => Err(InvalidValue("api key", invalid.to_string())),
        }
    }
}
impl From<ApiKey> for i16 {
    fn from(value: ApiKey) -> Self {
        match value {
            ApiKey::Produce => 0,
            ApiKey::Fetch => 1,
            ApiKey::Metadata => 3,
            ApiKey::OffsetCommit => 8,
            ApiKey::OffsetFetch => 9,
            ApiKey::FindCoordinator => 10,
            ApiKey::JoinGroup => 11,
            ApiKey::Heartbeat => 12,
            ApiKey::LeaveGroup => 13,
            ApiKey::SyncGroup => 14,
            ApiKey::ApiVersion => 18,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, Default)]
pub enum Acks {
    #[default]
    All,
    Leader,
    None,
}
impl From<Acks> for i16 {
    fn from(value: Acks) -> Self {
        match value {
            Acks::All => -1,
            Acks::Leader => 1,
            Acks::None => 0,
        }
    }
}

impl TryFrom<i16> for Acks {
    type Error = AppError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            -1 => Ok(Acks::All),
            1 => Ok(Acks::Leader),
            0 => Ok(Acks::None),
            invalid => Err(InvalidValue("ack field", invalid.to_string())),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum ApiVersion {
    #[default]
    V0,
    V1,
    V2,
    V3,
    V4,
    V5,
}

impl From<ApiVersion> for i16 {
    fn from(value: ApiVersion) -> Self {
        match value {
            ApiVersion::V0 => 0,
            ApiVersion::V1 => 1,
            ApiVersion::V2 => 2,
            ApiVersion::V3 => 3,
            ApiVersion::V4 => 4,
            ApiVersion::V5 => 5,
        }
    }
}
impl TryFrom<i16> for ApiVersion {
    type Error = AppError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ApiVersion::V0),
            1 => Ok(ApiVersion::V1),
            2 => Ok(ApiVersion::V2),
            3 => Ok(ApiVersion::V3),
            4 => Ok(ApiVersion::V4),
            5 => Ok(ApiVersion::V5),
            invalid => Err(InvalidValue("api version", invalid.to_string())),
        }
    }
}

pub enum ProtocolError {
    InvalidTopic(ErrorDetail),
}

pub struct ErrorDetail {
    pub code: i16,
    pub message: &'static str,
}
pub const INVALID_TOPIC_ERROR: ErrorDetail = ErrorDetail {
    code: 17,
    message: "Invalid topic",
};
