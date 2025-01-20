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

use crate::{
    request::kafka_errors::{KafkaError, KafkaResult},
    request::RequestContext,
};

use super::ApiHandler;

pub struct LeaveGroupRequestHandler;
impl ApiHandler for LeaveGroupRequestHandler {
    type Request = LeaveGroupRequest;
    type Response = LeaveGroupResponse;

    async fn handle_request(
        &self,
        request: LeaveGroupRequest,
        context: &RequestContext,
    ) -> LeaveGroupResponse {
        let response_result = context
            .group_coordinator
            .clone()
            .handle_group_leave(&request.group_id, &request.member_id)
            .await;
        LeaveGroupResponse::new(response_result)
    }
}

#[derive(Debug)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    pub member_id: String,
}

#[derive(Debug)]
pub struct LeaveGroupResponse {
    pub error: KafkaError,
    pub throttle_time_ms: i32,
}
impl LeaveGroupResponse {
    pub fn new(result: KafkaResult<()>) -> Self {
        match result {
            Ok(_) => LeaveGroupResponse {
                error: KafkaError::None,
                throttle_time_ms: 0,
            },
            Err(e) => LeaveGroupResponse {
                error: e,
                throttle_time_ms: 0,
            },
        }
    }
}
