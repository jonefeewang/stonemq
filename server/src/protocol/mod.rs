use std::clone::Clone;
use std::convert::TryFrom;
use std::sync::Arc;

use bytes::BytesMut;

use crate::{AppError, AppResult};
use crate::AppError::InvalidValue;
use crate::protocol::api_schemas::produce::{PRODUCE_REQUEST_SCHEMA_V0, PRODUCE_REQUEST_SCHEMA_V3};
use crate::protocol::schema::Schema;

mod api_schemas;
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
    fn write_to(self, buffer: &mut BytesMut, api_version: &ApiVersion) -> AppResult<()>;
    fn read_from(buffer: &mut BytesMut, api_version: &ApiVersion) -> AppResult<T>;

    fn fetch_schema_for_api(api_version: &ApiVersion, api_key: &ApiKey) -> Arc<Schema> {
        match api_key {
            ApiKey::Produce(_, _) => match api_version {
                ApiVersion::V0(_) | ApiVersion::V1(_) | ApiVersion::V2(_) => {
                    Arc::clone(&PRODUCE_REQUEST_SCHEMA_V0)
                }
                ApiVersion::V3(_) => Arc::clone(&PRODUCE_REQUEST_SCHEMA_V3),
                _ => {
                    todo!()
                }
            },
            ApiKey::Metadata(_, _) => {
                todo!()
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum ApiKey {
    Produce(i16, &'static str),
    Metadata(i16, &'static str),
}

#[derive(Debug, Clone)]
pub enum Acks {
    All(i16, &'static str),
    Leader(i16, &'static str),
    None(i16, &'static str),
}
impl PartialEq for Acks {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Acks::All(_, _), Acks::All(_, _)) => true,
            (Acks::Leader(_, _), Acks::Leader(_, _)) => true,
            (Acks::None(_, _), Acks::None(_, _)) => true,
            _ => false,
        }
    }
}
impl Eq for Acks {}

impl Default for Acks {
    fn default() -> Self {
        Acks::All(-1, "All")
    }
}
#[derive(Debug, Clone, Copy)]
pub enum ApiVersion {
    V0(i16),
    V1(i16),
    V2(i16),
    V3(i16),
}
impl Default for ApiVersion {
    fn default() -> Self {
        ApiVersion::V0(0)
    }
}

impl TryFrom<i16> for Acks {
    type Error = AppError;

    fn try_from(ack: i16) -> Result<Self, Self::Error> {
        match ack {
            -1 => Ok(Acks::All(-1, "ALL")),
            1 => Ok(Acks::Leader(1, "Leader")),
            0 => Ok(Acks::None(0, "None")),
            invalid => Err(InvalidValue("ack field", invalid.to_string())),
        }
    }
}
impl TryFrom<i16> for ApiKey {
    type Error = AppError;

    fn try_from(value: i16) -> AppResult<ApiKey> {
        match value {
            0 => Ok(ApiKey::Produce(0, "Produce")),
            1 => Ok(ApiKey::Metadata(3, "Metadata")),
            invalid => Err(InvalidValue("api key", invalid.to_string())),
        }
    }
}
impl TryFrom<i16> for ApiVersion {
    type Error = AppError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ApiVersion::V0(0)),
            1 => Ok(ApiVersion::V1(1)),
            invalid => Err(InvalidValue("api version", invalid.to_string())),
        }
    }
}
