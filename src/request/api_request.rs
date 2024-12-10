use bytes::BytesMut;

use crate::protocol::{ApiKey, ProtocolCodec};
use crate::request::{
    ApiRequest, ApiVersionRequest, FetchOffsetsRequest, FetchRequest, FindCoordinatorRequest,
    HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest, MetaDataRequest, OffsetCommitRequest,
    ProduceRequest, RequestHeader, SyncGroupRequest,
};

impl ApiRequest {
    pub fn parse_from(
        (mut request_body, request_header): (BytesMut, &RequestHeader),
    ) -> (Option<Self>, Option<BytesMut>) {
        match request_header.api_key {
            ApiKey::Produce => {
                let produce_request =
                    ProduceRequest::decode(&mut request_body, &request_header.api_version);
                if let Ok(produce_request) = produce_request {
                    (Some(ApiRequest::Produce(produce_request)), None)
                } else {
                    (None, None)
                }
            }
            ApiKey::Fetch => {
                let fetch_request =
                    FetchRequest::decode(&mut request_body, &request_header.api_version);
                if let Ok(fetch_request) = fetch_request {
                    (Some(ApiRequest::Fetch(fetch_request)), None)
                } else {
                    (None, None)
                }
            }
            ApiKey::Metadata => {
                let metadata_request =
                    MetaDataRequest::decode(&mut request_body, &request_header.api_version);
                if let Ok(metadata_request) = metadata_request {
                    (Some(ApiRequest::Metadata(metadata_request)), None)
                } else {
                    (None, None)
                }
            }
            ApiKey::ApiVersionKey => {
                let api_version_request = ApiVersionRequest::new(request_header.api_version);
                (Some(ApiRequest::ApiVersion(api_version_request)), None)
            }
            ApiKey::JoinGroup => {
                let join_group_request =
                    JoinGroupRequest::decode(&mut request_body, &request_header.api_version);
                if let Ok(join_group_request) = join_group_request {
                    (Some(ApiRequest::JoinGroup(join_group_request)), None)
                } else {
                    (None, None)
                }
            }
            ApiKey::SyncGroup => {
                let sync_group_request =
                    SyncGroupRequest::decode(&mut request_body, &request_header.api_version);
                if let Ok(sync_group_request) = sync_group_request {
                    (Some(ApiRequest::SyncGroup(sync_group_request)), None)
                } else {
                    (None, None)
                }
            }
            ApiKey::LeaveGroup => {
                let leave_group_request =
                    LeaveGroupRequest::decode(&mut request_body, &request_header.api_version);
                if let Ok(leave_group_request) = leave_group_request {
                    (Some(ApiRequest::LeaveGroup(leave_group_request)), None)
                } else {
                    (None, None)
                }
            }
            ApiKey::Heartbeat => {
                let heartbeat_request =
                    HeartbeatRequest::decode(&mut request_body, &request_header.api_version);
                if let Ok(heartbeat_request) = heartbeat_request {
                    (Some(ApiRequest::Heartbeat(heartbeat_request)), None)
                } else {
                    (None, None)
                }
            }
            ApiKey::OffsetCommit => {
                let offset_commit_request =
                    OffsetCommitRequest::decode(&mut request_body, &request_header.api_version);
                if let Ok(offset_commit_request) = offset_commit_request {
                    (Some(ApiRequest::OffsetCommit(offset_commit_request)), None)
                } else {
                    (None, None)
                }
            }
            ApiKey::OffsetFetch => {
                let fetch_offsets_request =
                    FetchOffsetsRequest::decode(&mut request_body, &request_header.api_version);
                if let Ok(fetch_offsets_request) = fetch_offsets_request {
                    (Some(ApiRequest::FetchOffsets(fetch_offsets_request)), None)
                } else {
                    (None, None)
                }
            }
            ApiKey::FindCoordinator => {
                let find_coordinator_request =
                    FindCoordinatorRequest::decode(&mut request_body, &request_header.api_version);
                if let Ok(find_coordinator_request) = find_coordinator_request {
                    (
                        Some(ApiRequest::FindCoordinator(find_coordinator_request)),
                        None,
                    )
                } else {
                    (None, None)
                }
            }
        }
    }
}
