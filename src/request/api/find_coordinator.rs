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

use crate::service::Node;

use crate::request::RequestContext;

use super::ApiHandler;

pub struct FindCoordinatorRequestHandler;
impl ApiHandler for FindCoordinatorRequestHandler {
    type Request = FindCoordinatorRequest;
    type Response = FindCoordinatorResponse;

    async fn handle_request(
        &self,
        request: FindCoordinatorRequest,
        context: &RequestContext,
    ) -> FindCoordinatorResponse {
        context.group_coordinator.find_coordinator(request).await
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct FindCoordinatorRequest {
    pub coordinator_key: String,
    pub coordinator_type: i8,
}

#[derive(Debug)]
pub struct FindCoordinatorResponse {
    pub throttle_time_ms: i32,
    pub error_message: Option<String>,
    pub error: i16,
    pub node: Node,
}

impl From<Node> for FindCoordinatorResponse {
    fn from(node: Node) -> Self {
        FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_message: None,
            error: 0,
            node,
        }
    }
}
