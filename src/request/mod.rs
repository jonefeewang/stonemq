mod api_version;
mod errors;
mod fetch;
mod fetch_offset;
mod find_coordinator;
mod heartbeat;
mod join_group;
mod leave_group;
mod metadata;
mod offset_commit;
mod produce;
mod request_context;
mod request_header;
mod request_processor;
mod sync_group;

pub use request_processor::RequestProcessor;

pub use errors::ErrorCode;
pub use errors::KafkaError;
pub use errors::KafkaResult;
pub use request_context::RequestContext;
pub use request_header::RequestHeader;

pub use api_version::{ApiVersionRequest, ApiVersionResponse};
pub use fetch::{FetchRequest, FetchResponse};
pub use fetch_offset::{FetchOffsetsRequest, FetchOffsetsResponse};
pub use find_coordinator::{FindCoordinatorRequest, FindCoordinatorResponse};
pub use heartbeat::{HeartbeatRequest, HeartbeatResponse};
pub use join_group::{JoinGroupRequest, JoinGroupResponse};
pub use leave_group::{LeaveGroupRequest, LeaveGroupResponse};
pub use metadata::{MetaDataRequest, MetadataResponse};
pub use offset_commit::{OffsetCommitRequest, OffsetCommitResponse};
pub use produce::{ProduceRequest, ProduceResponse};
pub use sync_group::{SyncGroupRequest, SyncGroupResponse};

pub use fetch::IsolationLevel;
pub use fetch::PartitionDataRep;
pub use fetch::PartitionDataReq;

pub use join_group::ProtocolMetadata;

pub use offset_commit::PartitionOffsetCommitData;

pub use fetch_offset::PartitionOffsetData;
pub use produce::PartitionResponse;

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
