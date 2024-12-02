use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use consumer_group::{
    FetchOffsetsRequest, FetchOffsetsResponse, FindCoordinatorRequest, HeartbeatRequest,
    HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse,
    OffsetCommitRequest, OffsetCommitResponse, SyncGroupRequest, SyncGroupResponse,
};
use errors::{ErrorCode, KafkaError};
use fetch::FetchRequest;
use tracing::{debug, instrument, trace};

use crate::message::GroupCoordinator;
use crate::protocol::api_schemas::SUPPORTED_API_VERSIONS;
use crate::protocol::{ApiKey, ApiVersion, ProtocolCodec};
use crate::request::metadata::{MetaDataRequest, MetadataResponse};
use crate::request::produce::{ProduceRequest, ProduceResponse};
use crate::service::Node;
use crate::ReplicaManager;

mod create_topic;

pub mod consumer_group;
pub mod errors;
pub mod fetch;
pub mod metadata;
pub mod produce;

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

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeader {
    // for test use
    pub fn new(
        api_key: ApiKey,
        api_version: ApiVersion,
        correlation_id: i32,
        client_id: Option<String>,
    ) -> Self {
        RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        }
    }
    pub fn size(&self) -> usize {
        match &self.client_id {
            Some(client_id) => {
                let client_id_size = client_id.len();
                2 + 2 + 4 + 4 + client_id_size
            }
            None => 2 + 2 + 4 + 4,
        }
    }
}

#[derive(Debug)]
pub struct RequestContext {
    pub replica_manager: Arc<ReplicaManager>,
    pub group_coordinator: Arc<GroupCoordinator>,
    pub request_header: RequestHeader,
}
impl RequestContext {
    pub fn new(
        request_header: RequestHeader,
        replica_manager: Arc<ReplicaManager>,
        group_coordinator: Arc<GroupCoordinator>,
    ) -> Self {
        RequestContext {
            request_header,
            replica_manager,
            group_coordinator,
        }
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
        ApiVersionResponse {
            error_code: 0,
            throttle_time_ms: 0,
            api_versions,
        }
    }
}

#[derive(Debug)]
pub struct ApiVersionResponse {
    pub error_code: i16,
    pub throttle_time_ms: i32,
    pub api_versions: HashMap<i16, (i16, i16)>,
}

#[derive(Debug)]
pub struct RequestProcessor {}

impl RequestProcessor {
    pub async fn process_request(
        request: ApiRequest,
        client_ip: String,
        request_header: &RequestHeader,
        replica_manager: Arc<ReplicaManager>,
        group_coordinator: Arc<GroupCoordinator>,
    ) -> BytesMut {
        trace!(
            "Processing request: {:?} with request header{:?}",
            request,
            request_header
        );
        match request {
            ApiRequest::Produce(request) => {
                Self::handle_produce_request(request, request_header, client_ip, replica_manager)
                    .await
            }
            ApiRequest::Fetch(request) => {
                Self::handle_fetch_request(request_header, request, client_ip, replica_manager)
                    .await
            }

            ApiRequest::Metadata(request) => {
                Self::handle_metadata_request(request_header, request, client_ip, replica_manager)
                    .await
            }
            ApiRequest::ApiVersion(request) => {
                Self::handle_api_version_request(request_header, request, client_ip).await
            }
            ApiRequest::FindCoordinator(request) => {
                Self::handle_find_coordinator_request(
                    request_header,
                    request,
                    client_ip,
                    group_coordinator,
                )
                .await
            }
            ApiRequest::JoinGroup(request) => {
                // join group 的信号在handle_join_group_request中发送
                Self::handle_join_group_request(
                    request_header,
                    request,
                    client_ip,
                    group_coordinator,
                )
                .await
            }
            ApiRequest::SyncGroup(request) => {
                Self::handle_sync_group_request(
                    request_header,
                    request,
                    client_ip,
                    group_coordinator,
                )
                .await
            }
            ApiRequest::LeaveGroup(request) => {
                Self::handle_leave_group_request(
                    request_header,
                    request,
                    client_ip,
                    group_coordinator,
                )
                .await
            }
            ApiRequest::Heartbeat(request) => {
                Self::handle_heartbeat_request(
                    request_header,
                    request,
                    client_ip,
                    group_coordinator,
                )
                .await
            }
            ApiRequest::OffsetCommit(request) => {
                Self::handle_offset_commit_request(
                    request_header,
                    request,
                    client_ip,
                    group_coordinator,
                )
                .await
            }
            ApiRequest::FetchOffsets(request) => {
                Self::handle_fetch_offsets_request(
                    request_header,
                    request,
                    client_ip,
                    group_coordinator,
                )
                .await
            }
        }
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            client_id = request_header.client_id,
            correlation_id = request_header.correlation_id,
            client_host = client_ip,
        )
    )]
    pub async fn handle_produce_request(
        produce_request: ProduceRequest,
        request_header: &RequestHeader,
        client_ip: String,
        replica_manager: Arc<ReplicaManager>,
    ) -> BytesMut {
        let tp_response = replica_manager
            .append_records(produce_request.topic_data)
            .await;
        let response = ProduceResponse {
            responses: tp_response,
            throttle_time: None,
        };
        trace!("produce response: {:?}", response);

        response.encode(&request_header.api_version, request_header.correlation_id)
    }
    #[instrument(
        level = "trace",
        skip_all,
        fields(
            client_id = request_header.client_id,
            correlation_id = request_header.correlation_id,
            client_host = client_ip,
        )
    )]
    pub async fn handle_fetch_request(
        request_header: &RequestHeader,
        request: FetchRequest,
        client_ip: String,
        replica_manager: Arc<ReplicaManager>,
    ) -> BytesMut {
        debug!("fetch request: {:?}", request);
        let fetch_response = replica_manager.fetch_message(request).await;
        debug!("fetch response: {:?}", fetch_response);
        fetch_response.encode(&request_header.api_version, request_header.correlation_id)
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            client_id = request_header.client_id,
            correlation_id = request_header.correlation_id,
            client_host = client_ip,
        )
    )]
    pub async fn handle_api_version_request(
        request_header: &RequestHeader,
        request: ApiVersionRequest,
        client_ip: String,
    ) -> BytesMut {
        let response = request.process();
        debug!("api versions request");
        response.encode(&request_header.api_version, request_header.correlation_id)
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            client_id = request_header.client_id,
            correlation_id = request_header.correlation_id,
            client_host = client_ip,
        )
    )]
    pub async fn handle_metadata_request(
        request_header: &RequestHeader,
        request: MetaDataRequest,
        client_ip: String,
        replica_manager: Arc<ReplicaManager>,
    ) -> BytesMut {
        // send metadata for specific topics
        debug!("metadata request");

        let metadata = replica_manager.get_queue_metadata(
            request
                .topics
                .map(|v| v.into_iter().map(Cow::Owned).collect()),
        );
        // send metadata to client
        let metadata_reps = MetadataResponse::new(metadata, Node::new_localhost());
        metadata_reps.encode(&request_header.api_version, request_header.correlation_id)
    }
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            client_id = request_header.client_id,
            correlation_id = request_header.correlation_id,
            client_host = client_ip,
        )
    )]
    pub async fn handle_find_coordinator_request(
        request_header: &RequestHeader,
        request: FindCoordinatorRequest,
        client_ip: String,
        group_coordinator: Arc<GroupCoordinator>,
    ) -> BytesMut {
        let response = group_coordinator.find_coordinator(request).await;
        response.encode(&request_header.api_version, request_header.correlation_id)
    }
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            client_id = request_header.client_id,
            correlation_id = request_header.correlation_id,
            client_host = client_ip,
        )
    )]
    pub async fn handle_join_group_request(
        request_header: &RequestHeader,
        mut request: JoinGroupRequest,
        client_ip: String,
        group_coordinator: Arc<GroupCoordinator>,
    ) -> BytesMut {
        if let Some(client_id) = request_header.client_id.clone() {
            request.client_id = client_id;
        }
        let join_result = group_coordinator.handle_join_group(request).await;

        let join_group_response = JoinGroupResponse::new(
            join_result.error as i16,
            join_result.generation_id,
            join_result.sub_protocol,
            join_result.member_id,
            join_result.leader_id,
            join_result.members,
        );
        debug!("join group response");
        join_group_response.encode(&request_header.api_version, request_header.correlation_id)
    }
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            client_id = request_header.client_id,
            correlation_id = request_header.correlation_id,
            client_host = client_ip,
        )
    )]
    pub async fn handle_sync_group_request(
        request_header: &RequestHeader,
        request: SyncGroupRequest,
        client_ip: String,
        group_coordinator: Arc<GroupCoordinator>,
    ) -> BytesMut {
        debug!("sync group request: {:?}", request);
        // request 中的bytesmut 来自于connection中的buffer，不能破坏掉，需要返还给connection，这里将BytesMut转换成Bytes，返还BytesMut
        let group_assignment = request
            .group_assignment
            .iter()
            .map(|(k, v)| (k.clone(), Bytes::from(v.clone())))
            .collect();
        let sync_group_result = group_coordinator
            .handle_sync_group(
                &request.group_id,
                &request.member_id,
                request.generation_id,
                group_assignment,
            )
            .await;
        debug!("sync group response: {:?}", sync_group_result);
        match sync_group_result {
            Ok(response) => {
                response.encode(&request_header.api_version, request_header.correlation_id)
            }
            Err(e) => {
                let error_code = ErrorCode::from(&e);
                SyncGroupResponse::new(error_code, 0, Bytes::new())
                    .encode(&request_header.api_version, request_header.correlation_id)
            }
        }
    }
    pub async fn handle_leave_group_request(
        request_header: &RequestHeader,
        request: LeaveGroupRequest,
        _client_ip: String,
        group_coordinator: Arc<GroupCoordinator>,
    ) -> BytesMut {
        let response_result = group_coordinator
            .handle_group_leave(&request.group_id, &request.member_id)
            .await;
        let response = LeaveGroupResponse::new(response_result);

        response.encode(&request_header.api_version, request_header.correlation_id)
    }
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            client_id = request_header.client_id,
            correlation_id = request_header.correlation_id,
            client_host = client_ip,
        )
    )]
    pub async fn handle_heartbeat_request(
        request_header: &RequestHeader,
        request: HeartbeatRequest,
        client_ip: String,
        group_coordinator: Arc<GroupCoordinator>,
    ) -> BytesMut {
        debug!("received heartbeat request");
        let result = group_coordinator
            .handle_heartbeat(&request.group_id, &request.member_id, request.generation_id)
            .await;
        let response = match result {
            Ok(_) => HeartbeatResponse::new(KafkaError::None, 0),
            Err(e) => HeartbeatResponse::new(e, 0),
        };

        response.encode(&request_header.api_version, request_header.correlation_id)
    }
    #[instrument(
        level = "trace",
        skip_all,
        fields(
            client_id = request_header.client_id,
            correlation_id = request_header.correlation_id,
            client_host = client_ip,
        )
    )]
    pub async fn handle_offset_commit_request(
        request_header: &RequestHeader,
        request: OffsetCommitRequest,
        client_ip: String,
        group_coordinator: Arc<GroupCoordinator>,
    ) -> BytesMut {
        debug!("offset commit request: {:?}", request);
        let result = group_coordinator
            .handle_commit_offsets(
                &request.group_id,
                &request.member_id,
                request.generation_id,
                request.offset_data,
            )
            .await;

        debug!("offset commit result: {:?}", result);
        let response = OffsetCommitResponse::new(0, result);
        response.encode(&request_header.api_version, request_header.correlation_id)
    }
    #[instrument(
        level = "trace",
        skip_all,
        fields(
            client_id = request_header.client_id,
            correlation_id = request_header.correlation_id,
            client_host = client_ip,
        )
    )]
    pub async fn handle_fetch_offsets_request(
        request_header: &RequestHeader,
        request: FetchOffsetsRequest,
        client_ip: String,
        group_coordinator: Arc<GroupCoordinator>,
    ) -> BytesMut {
        let result = group_coordinator.handle_fetch_offsets(&request.group_id, request.partitions);
        debug!("fetch offsets result: {:?}", result);
        if let Ok(offsets) = result {
            let response = FetchOffsetsResponse::new(KafkaError::None, offsets);
            response.encode(&request_header.api_version, request_header.correlation_id)
        } else {
            FetchOffsetsResponse::new(result.err().unwrap(), HashMap::new())
                .encode(&request_header.api_version, request_header.correlation_id)
        }
    }
}
