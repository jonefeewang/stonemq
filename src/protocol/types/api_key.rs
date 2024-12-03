use crate::{AppError, AppResult};

use crate::protocol::base::{ProtocolType, I16};

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
    Produce = 0,
    Fetch = 1,
    Metadata = 3,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    ApiVersionKey = 18,
}

impl ApiKey {
    pub fn from_i16(value: i16) -> AppResult<Self> {
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
            18 => Ok(ApiKey::ApiVersionKey),
            invalid => Err(AppError::MalformedProtocol(format!(
                "api key:{} is invalid",
                invalid
            ))),
        }
    }

    pub fn as_i16(&self) -> i16 {
        *self as i16
    }
}

impl From<ApiKey> for ProtocolType {
    fn from(value: ApiKey) -> Self {
        ProtocolType::I16(I16 {
            value: value as i16,
        })
    }
}
