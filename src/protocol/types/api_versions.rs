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

use crate::{AppError, AppResult};

use crate::protocol::base::{ProtocolType, I16};

#[derive(Debug, Clone, Copy, Default)]
pub enum ApiVersion {
    #[default]
    V0 = 0,
    V1 = 1,
    V2 = 2,
    V3 = 3,
    V4 = 4,
    V5 = 5,
}

impl ApiVersion {
    pub fn from_i16(value: i16) -> AppResult<Self> {
        match value {
            0 => Ok(ApiVersion::V0),
            1 => Ok(ApiVersion::V1),
            2 => Ok(ApiVersion::V2),
            3 => Ok(ApiVersion::V3),
            4 => Ok(ApiVersion::V4),
            5 => Ok(ApiVersion::V5),
            invalid => Err(AppError::MalformedProtocol(format!(
                "api version:{} is invalid",
                invalid
            ))),
        }
    }

    pub fn as_i16(&self) -> i16 {
        *self as i16
    }
}

impl From<ApiVersion> for ProtocolType {
    fn from(value: ApiVersion) -> Self {
        ProtocolType::I16(I16 {
            value: value as i16,
        })
    }
}
