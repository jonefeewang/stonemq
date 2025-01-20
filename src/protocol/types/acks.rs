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

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, Default)]
pub enum Acks {
    #[default]
    All = -1,
    Leader = 1,
    None = 0,
}

impl Acks {
    pub fn from_i16(value: i16) -> AppResult<Self> {
        match value {
            -1 => Ok(Acks::All),
            1 => Ok(Acks::Leader),
            0 => Ok(Acks::None),
            invalid => Err(AppError::MalformedProtocol(format!(
                "ack field:{} is invalid",
                invalid
            ))),
        }
    }
    pub fn as_i16(&self) -> i16 {
        *self as i16
    }
}

impl From<Acks> for ProtocolType {
    fn from(value: Acks) -> Self {
        ProtocolType::I16(I16 {
            value: value as i16,
        })
    }
}
