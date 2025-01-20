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

use bytes::BytesMut;
use tracing::trace;

use crate::protocol::ProtocolCodec;
use crate::request::api::ApiVersionRequestHandler;
use crate::request::api::FetchOffsetsRequestHandler;
use crate::request::api::FetchRequestHandler;
use crate::request::api::FindCoordinatorRequestHandler;
use crate::request::api::HeartbeatRequestHandler;
use crate::request::api::JoinGroupRequestHandler;
use crate::request::api::LeaveGroupRequestHandler;
use crate::request::api::MetadataRequestHandler;
use crate::request::api::OffsetCommitRequestHandler;
use crate::request::api::ProduceRequestHandler;
use crate::request::api::SyncGroupRequestHandler;

use crate::request::ApiRequest;
use crate::request::RequestContext;

use super::api::ApiHandler;

/// general async handler
async fn execute_handler<H>(handler: H, request: H::Request, context: &RequestContext) -> BytesMut
where
    H: ApiHandler + Sync,
{
    // call the specific handler to generate the response
    let response = handler.handle_request(request, context).await;

    // encode the response to byte stream
    response.encode(
        &context.request_header.api_version,
        context.request_header.correlation_id,
    )
}

pub struct RequestProcessor;

impl RequestProcessor {
    pub async fn process_request(request: ApiRequest, context: &RequestContext) -> BytesMut {
        trace!(
            "Processing request: {:?} with request header{:?}",
            request,
            context.request_header
        );
        match request {
            ApiRequest::Produce(request) => {
                let handler = ProduceRequestHandler;
                execute_handler(handler, request, context).await
            }
            ApiRequest::Fetch(request) => {
                let handler = FetchRequestHandler;
                execute_handler(handler, request, context).await
            }
            ApiRequest::Metadata(request) => {
                let handler = MetadataRequestHandler;
                execute_handler(handler, request, context).await
            }
            ApiRequest::ApiVersion(request) => {
                let handler = ApiVersionRequestHandler;
                execute_handler(handler, request, context).await
            }
            ApiRequest::FindCoordinator(request) => {
                let handler = FindCoordinatorRequestHandler;
                execute_handler(handler, request, context).await
            }
            ApiRequest::JoinGroup(request) => {
                let handler = JoinGroupRequestHandler;
                execute_handler(handler, request, context).await
            }
            ApiRequest::SyncGroup(request) => {
                let handler = SyncGroupRequestHandler;
                execute_handler(handler, request, context).await
            }
            ApiRequest::LeaveGroup(request) => {
                let handler = LeaveGroupRequestHandler;
                execute_handler(handler, request, context).await
            }
            ApiRequest::Heartbeat(request) => {
                let handler = HeartbeatRequestHandler;
                execute_handler(handler, request, context).await
            }
            ApiRequest::OffsetCommit(request) => {
                let handler = OffsetCommitRequestHandler;
                execute_handler(handler, request, context).await
            }
            ApiRequest::FetchOffsets(request) => {
                let handler = FetchOffsetsRequestHandler;
                execute_handler(handler, request, context).await
            }
        }
    }
}
