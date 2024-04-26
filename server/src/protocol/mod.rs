use std::clone::Clone;
use std::convert::TryFrom;

use bytes::{Buf, BufMut};
use integer_encoding::VarInt;

use crate::protocol::Acks::{All, Leader};
use crate::protocol::ApiKey::{Metadata, Produce};
use crate::AppError::InvalidValue;
use crate::{AppError, AppResult};

mod api_schemas;
mod array;
mod field;
mod primary_types;
mod protocol;
mod record;
mod schema;
mod structure;
mod types;
mod utils;

#[derive(Debug, Copy, Clone, Default)]
pub enum ApiKey {
    #[default]
    Produce,
    Metadata,
}

#[derive(Clone)]
pub enum Acks {
    All(i8),
    Leader(i8),
    None(i8),
}

impl Default for Acks {
    fn default() -> Self {
        Acks::All(-1)
    }
}
#[derive(Debug, Clone, Copy, Default)]
pub enum ApiVersion {
    #[default]
    V0,
    V1,
    V2,
    V3,
}

impl TryFrom<i16> for Acks {
    type Error = AppError;

    fn try_from(ack: i16) -> Result<Self, Self::Error> {
        match ack {
            -1 => Ok(All(-1)),
            1 => Ok(Leader(1)),
            0 => Ok(Acks::None(0)),
            invalid => Err(InvalidValue("ack field", invalid.to_string())),
        }
    }
}
impl TryFrom<i16> for ApiKey {
    type Error = AppError;

    fn try_from(value: i16) -> AppResult<ApiKey> {
        match value {
            0 => Ok(Produce),
            1 => Ok(Metadata),
            invalid => Err(InvalidValue("api key", invalid.to_string())),
        }
    }
}
