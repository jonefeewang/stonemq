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

use bytes::{Bytes, BytesMut};
use tracing::debug;

use crate::request::kafka_errors::ErrorCode;
use crate::request::RequestContext;

use super::ApiHandler;

pub struct SyncGroupRequestHandler;
impl ApiHandler for SyncGroupRequestHandler {
    type Request = SyncGroupRequest;
    type Response = SyncGroupResponse;

    async fn handle_request(
        &self,
        request: SyncGroupRequest,
        context: &RequestContext,
    ) -> SyncGroupResponse {
        debug!("sync group request: {:?}", request);
        // the BytesMut in request comes from the buffer of the connection, it cannot be destroyed,
        // it needs to be returned to the connection, here BytesMut is converted to Bytes, and returned to BytesMut
        let group_assignment = request
            .group_assignment
            .iter()
            .map(|(k, v)| (k.clone(), Bytes::from(v.clone())))
            .collect();
        let sync_group_result = context
            .group_coordinator
            .clone()
            .handle_sync_group(
                &request.group_id,
                &request.member_id,
                request.generation_id,
                group_assignment,
            )
            .await;
        debug!("sync group response: {:?}", sync_group_result);
        match sync_group_result {
            Ok(response) => response,
            Err(e) => {
                let error_code = ErrorCode::from(&e);
                SyncGroupResponse::new(error_code, 0, Bytes::new())
            }
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

#[derive(Debug)]
pub struct SyncGroupResponse {
    pub error_code: ErrorCode,
    pub throttle_time_ms: i32,
    pub member_assignment: Bytes,
}
impl SyncGroupResponse {
    pub fn new(error_code: ErrorCode, throttle_time_ms: i32, member_assignment: Bytes) -> Self {
        SyncGroupResponse {
            error_code,
            throttle_time_ms,
            member_assignment,
        }
    }
}
