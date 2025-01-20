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

mod api;
mod api_request;
mod kafka_errors;
mod request_context;
mod request_header;
mod request_processor;

// request processor and utility
pub use kafka_errors::ErrorCode;
pub use kafka_errors::KafkaError;
pub use kafka_errors::KafkaResult;
pub use request_context::RequestContext;
pub use request_header::RequestHeader;
pub use request_processor::RequestProcessor;

// utility value object for request and response
pub use api::IsolationLevel;
pub use api::PartitionDataRep;
pub use api::PartitionDataReq;
pub use api::PartitionOffsetCommitData;
pub use api::PartitionOffsetData;
pub use api::PartitionResponse;
pub use api::ProtocolMetadata;

// request and response
pub use api::{ApiVersionRequest, ApiVersionResponse};
pub use api::{FetchOffsetsRequest, FetchOffsetsResponse};
pub use api::{FetchRequest, FetchResponse};
pub use api::{FindCoordinatorRequest, FindCoordinatorResponse};
pub use api::{HeartbeatRequest, HeartbeatResponse};
pub use api::{JoinGroupRequest, JoinGroupResponse};
pub use api::{LeaveGroupRequest, LeaveGroupResponse};
pub use api::{MetaDataRequest, MetadataResponse};
pub use api::{OffsetCommitRequest, OffsetCommitResponse};
pub use api::{ProduceRequest, ProduceResponse};
pub use api::{SyncGroupRequest, SyncGroupResponse};

// request enum
#[derive(Debug)]
pub enum ApiRequest {
    Produce(ProduceRequest),
    Fetch(FetchRequest),
    Metadata(MetaDataRequest),
    ApiVersion(ApiVersionRequest),
    FindCoordinator(FindCoordinatorRequest),
    JoinGroup(JoinGroupRequest),
    SyncGroup(SyncGroupRequest),
    LeaveGroup(LeaveGroupRequest),
    Heartbeat(HeartbeatRequest),
    OffsetCommit(OffsetCommitRequest),
    FetchOffsets(FetchOffsetsRequest),
}
