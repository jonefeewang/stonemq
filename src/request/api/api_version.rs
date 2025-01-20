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

use std::collections::HashMap;

use crate::{
    protocol::{ApiVersion, SUPPORTED_API_VERSIONS},
    request::{ErrorCode, RequestContext},
};

use super::handler::ApiHandler;

pub struct ApiVersionRequestHandler;
impl ApiHandler for ApiVersionRequestHandler {
    type Request = ApiVersionRequest;
    type Response = ApiVersionResponse;

    async fn handle_request(
        &self,
        request: ApiVersionRequest,
        _context: &RequestContext,
    ) -> ApiVersionResponse {
        request.process()
    }
}

#[derive(Debug)]
pub struct ApiVersionRequest {
    _version: ApiVersion,
}

impl ApiVersionRequest {
    pub fn new(_version: ApiVersion) -> Self {
        ApiVersionRequest { _version }
    }
    pub fn process(&self) -> ApiVersionResponse {
        let mut api_versions = HashMap::new();
        for (key, value) in SUPPORTED_API_VERSIONS.iter() {
            api_versions.insert(
                *key,
                (
                    *value.first().unwrap() as i16,
                    *value.last().unwrap() as i16,
                ),
            );
        }
        if self._version.as_i16() > ApiVersion::V2.as_i16() {
            ApiVersionResponse {
                error_code: ErrorCode::UnsupportedVersion as i16,
                throttle_time_ms: 0,
                api_versions,
            }
        } else {
            ApiVersionResponse {
                error_code: 0,
                throttle_time_ms: 0,
                api_versions,
            }
        }
    }
}

#[derive(Debug)]
pub struct ApiVersionResponse {
    pub error_code: i16,
    pub throttle_time_ms: i32,
    pub api_versions: HashMap<i16, (i16, i16)>,
}
