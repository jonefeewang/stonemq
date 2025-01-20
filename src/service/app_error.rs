// Copyright 2025 jonefeewang@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::request::KafkaError;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum AppError {
    /// general errors
    #[error("illegal state: {0}")]
    IllegalStateError(String),

    #[error("malformed protocol : {0}")]
    MalformedProtocol(String),

    #[error("invalid value: {0}")]
    InvalidValue(String),

    #[error("I/O error: {0}")]
    DetailedIoError(String),

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("channel send error: {0}")]
    ChannelSendError(String),

    #[error("channel recv error: {0}")]
    ChannelRecvError(String),

    #[error("invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Accept error = {0}")]
    Accept(String),

    #[error("config file error: {0}")]
    ConfigFileError(#[from] config::ConfigError),

    /// marker error    
    Incomplete,

    /// kafka protocol errors
    #[error("unknown error: {0}")]
    Unknown(String),

    #[error("corrupt message: {0}")]
    CorruptMessage(String),

    #[error("message too large: {0}")]
    MessageTooLarge(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("invalid topic: {0}")]
    InvalidTopic(String),

    #[error("unsupported api version: {0}")]
    UnsupportedVersion(i16),
}

impl From<AppError> for KafkaError {
    fn from(value: AppError) -> Self {
        match value {
            AppError::Unknown(s) => KafkaError::Unknown(s),
            AppError::CorruptMessage(s) => KafkaError::CorruptMessage(s),
            AppError::MessageTooLarge(s) => KafkaError::MessageTooLarge(s),
            AppError::InvalidRequest(s) => KafkaError::InvalidRequest(s),
            AppError::InvalidTopic(s) => KafkaError::InvalidTopic(s),
            AppError::UnsupportedVersion(s) => KafkaError::UnsupportedVersion(s.to_string()),
            _ => KafkaError::Unknown(value.to_string()),
        }
    }
}
