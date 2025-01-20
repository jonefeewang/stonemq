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

use std::collections::BTreeMap;

use bytes::{Bytes, BytesMut};
use tracing::debug;

use crate::request::RequestContext;

use super::ApiHandler;

pub struct JoinGroupRequestHandler;
impl ApiHandler for JoinGroupRequestHandler {
    type Request = JoinGroupRequest;
    type Response = JoinGroupResponse;

    async fn handle_request(
        &self,
        mut request: JoinGroupRequest,
        context: &RequestContext,
    ) -> JoinGroupResponse {
        if let Some(client_id) = context.request_header.client_id.clone() {
            request.client_id = client_id;
        }
        let join_result = context
            .group_coordinator
            .clone()
            .handle_join_group(request)
            .await;

        let join_group_response = JoinGroupResponse::new(
            join_result.error as i16,
            join_result.generation_id,
            join_result.sub_protocol,
            join_result.member_id,
            join_result.leader_id,
            join_result.members,
        );
        debug!("join group response");
        join_group_response
    }
}

#[derive(Debug)]
pub struct ProtocolMetadata {
    // assignor.name
    pub name: String,
    // consumer protocol serialized subscription struct
    pub metadata: BytesMut,
}
impl PartialEq for ProtocolMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.metadata == other.metadata
    }
}

#[derive(Debug)]
pub struct JoinGroupRequest {
    pub client_id: String,
    pub client_host: String,
    pub group_id: String,
    pub session_timeout: i32,
    pub rebalance_timeout: i32,
    pub member_id: String,
    pub protocol_type: String,
    pub group_protocols: Vec<ProtocolMetadata>,
}

impl JoinGroupRequest {}

#[derive(Debug)]
pub struct JoinGroupResponse {
    pub throttle_time: Option<i32>,
    // possible error codes:
    // COORDINATOR_LOAD_IN_PROGRESS (14)
    // GROUP_COORDINATOR_NOT_AVAILABLE (15)
    // NOT_COORDINATOR (16)
    // INCONSISTENT_GROUP_PROTOCOL (23)
    // UNKNOWN_MEMBER_ID (25)
    // INVALID_SESSION_TIMEOUT (26)
    // GROUP_AUTHORIZATION_FAILED (30)
    pub error_code: i16,
    pub generation_id: i32,
    pub group_protocol: String,
    pub member_id: String,
    pub leader_id: String,
    pub members: BTreeMap<String, Bytes>,
}

impl JoinGroupResponse {
    pub fn new(
        error: i16,
        generation_id: i32,
        group_protocol: String,
        member_id: String,
        leader_id: String,
        members: BTreeMap<String, Bytes>,
    ) -> Self {
        JoinGroupResponse {
            throttle_time: None,
            error_code: error,
            generation_id,
            group_protocol,
            member_id,
            leader_id,
            members,
        }
    }
}
