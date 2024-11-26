use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use consumer_group::{
    FetchOffsetsRequest, FetchOffsetsResponse, FindCoordinatorRequest, FindCoordinatorResponse,
    HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
    OffsetCommitRequest, OffsetCommitResponse, SyncGroupRequest, SyncGroupResponse,
};
use errors::{ErrorCode, KafkaError};
use fetch::FetchRequest;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, instrument, trace};

use crate::message::GroupCoordinator;
use crate::network::Connection;
use crate::protocol::api_schemas::SUPPORTED_API_VERSIONS;
use crate::protocol::{ApiKey, ApiVersion, ProtocolCodec};
use crate::request::metadata::{MetaDataRequest, MetadataResponse};
use crate::request::produce::{ProduceRequest, ProduceResponse};
use crate::service::Node;
use crate::AppError::IllegalStateError;

use crate::{AppError, AppResult, ReplicaManager};

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
pub struct RequestContext<'c> {
    pub conn: &'c mut Connection,
    pub request_header: RequestHeader,
    socket_read_ch_tx: mpsc::Sender<()>,
    pub replica_manager: Arc<ReplicaManager>,
    pub group_coordinator: Arc<GroupCoordinator>,
}
impl<'c> RequestContext<'c> {
    pub fn new(
        conn: &'c mut Connection,
        request_header: RequestHeader,
        socket_read_ch_tx: mpsc::Sender<()>,
        replica_manager: Arc<ReplicaManager>,
        group_coordinator: Arc<GroupCoordinator>,
    ) -> Self {
        RequestContext {
            conn,
            request_header,
            socket_read_ch_tx,
            replica_manager,
            group_coordinator,
        }
    }
    pub async fn notify_processor_proceed(&self) {
        let sender = &self.socket_read_ch_tx;
        let _ = sender.send(()).await;
    }
}

#[derive(Debug)]
pub struct ApiVersionRequest {
    version: ApiVersion,
}

impl ApiVersionRequest {
    pub fn new(version: ApiVersion) -> Self {
        ApiVersionRequest { version }
    }
    pub fn process(&self) -> AppResult<ApiVersionResponse> {
        let mut api_versions = HashMap::new();
        let error = || IllegalStateError(Cow::Borrowed("request api key dose not have a version"));
        for (key, value) in SUPPORTED_API_VERSIONS.iter() {
            api_versions.insert(
                *key,
                (
                    *value.first().ok_or_else(error)? as i16,
                    *value.last().ok_or_else(error)? as i16,
                ),
            );
        }
        Ok(ApiVersionResponse {
            error_code: 0,
            throttle_time_ms: 0,
            api_versions,
        })
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
        request_context: &mut RequestContext<'_>,
    ) -> AppResult<()> {
        trace!(
            "Processing request: {:?} with request header{:?}",
            request,
            &request_context.request_header
        );
        match request {
            ApiRequest::Produce(request) => {
                Self::handle_produce_request(request_context, request).await?;
                request_context.socket_read_ch_tx.send(()).await?;
                Ok(())
            }
            ApiRequest::Fetch(request) => {
                Self::handle_fetch_request(request_context, request).await?;
                request_context.socket_read_ch_tx.send(()).await?;
                Ok(())
            }
            ApiRequest::Metadata(request) => {
                Self::handle_metadata_request(request_context, request).await?;
                request_context.socket_read_ch_tx.send(()).await?;
                Ok(())
            }
            ApiRequest::ApiVersion(request) => {
                Self::handle_api_version_request(request_context, request).await?;
                request_context.socket_read_ch_tx.send(()).await?;
                Ok(())
            }
            ApiRequest::FindCoordinator(request) => {
                Self::handle_find_coordinator_request(request_context, request).await?;
                request_context.socket_read_ch_tx.send(()).await?;
                Ok(())
            }
            ApiRequest::JoinGroup(request) => {
                // join group 的信号在handle_join_group_request中发送
                Self::handle_join_group_request(request_context, request).await?;
                Ok(())
            }
            ApiRequest::SyncGroup(request) => {
                Self::handle_sync_group_request(request_context, request).await?;

                Ok(())
            }
            ApiRequest::LeaveGroup(request) => {
                Self::handle_leave_group_request(request_context, request).await?;
                request_context.socket_read_ch_tx.send(()).await?;
                Ok(())
            }
            ApiRequest::Heartbeat(request) => {
                Self::handle_heartbeat_request(request_context, request).await?;
                request_context.socket_read_ch_tx.send(()).await?;
                Ok(())
            }
            ApiRequest::OffsetCommit(request) => {
                Self::handle_offset_commit_request(request_context, request).await?;
                request_context.socket_read_ch_tx.send(()).await?;
                Ok(())
            }
            ApiRequest::FetchOffsets(request) => {
                Self::handle_fetch_offsets_request(request_context, request).await?;
                request_context.socket_read_ch_tx.send(()).await?;
                Ok(())
            }
        }
    }

    #[instrument(
        level = "trace",
        skip_all,
        fields(
            client_id = request_context.request_header.client_id,
            correlation_id = request_context.request_header.correlation_id,
            client_host = request_context.conn.client_ip,
        )
    )]
    pub async fn handle_produce_request(
        request_context: &mut RequestContext<'_>,
        request: ProduceRequest,
    ) -> AppResult<()> {
        let tp_response = request_context
            .replica_manager
            .append_records(request.topic_data)
            .await?;
        let response = ProduceResponse {
            responses: tp_response,
            throttle_time: None,
        };
        trace!("produce response: {:?}", response);

        response
            .write_to(
                &mut request_context.conn.writer,
                &request_context.request_header.api_version,
                request_context.request_header.correlation_id,
            )
            .await?;
        Ok(())
    }
    #[instrument(
        level = "trace",
        skip_all,
        fields(
            client_id = request_context.request_header.client_id,
            correlation_id = request_context.request_header.correlation_id,
            client_host = request_context.conn.client_ip,
        )
    )]
    pub async fn handle_fetch_request(
        request_context: &mut RequestContext<'_>,
        request: FetchRequest,
    ) -> AppResult<()> {
        debug!("fetch request: {:?}", request);
        let fetch_response = request_context
            .replica_manager
            .clone()
            .fetch_message(request)
            .await?;
        debug!("fetch response: {:?}", fetch_response);
        fetch_response
            .write_to(
                &mut request_context.conn.writer,
                &request_context.request_header.api_version,
                request_context.request_header.correlation_id,
            )
            .await?;
        Ok(())
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            client_id = request_context.request_header.client_id,
            correlation_id = request_context.request_header.correlation_id,
            client_host = request_context.conn.client_ip,
        )
    )]
    pub async fn handle_api_version_request(
        request_context: &mut RequestContext<'_>,
        request: ApiVersionRequest,
    ) -> AppResult<()> {
        let response = request.process()?;
        debug!("api versions request");
        response
            .write_to(
                &mut request_context.conn.writer,
                &request_context.request_header.api_version,
                request_context.request_header.correlation_id,
            )
            .await?;
        Ok(())
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            client_id = request_context.request_header.client_id,
            correlation_id = request_context.request_header.correlation_id,
            client_host = request_context.conn.client_ip,
        )
    )]
    pub async fn handle_metadata_request(
        request_context: &mut RequestContext<'_>,
        request: MetaDataRequest,
    ) -> AppResult<()> {
        // send metadata for specific topics
        debug!("metadata request");

        let metadata = request_context.replica_manager.get_queue_metadata(
            request
                .topics
                .map(|v| v.into_iter().map(Cow::Owned).collect()),
        );
        // send metadata to client
        let metadata_reps = MetadataResponse::new(metadata, Node::new_localhost());
        metadata_reps
            .write_to(
                &mut request_context.conn.writer,
                &request_context.request_header.api_version,
                request_context.request_header.correlation_id,
            )
            .await?;
        Ok(())
    }
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            client_id = request_context.request_header.client_id,
            correlation_id = request_context.request_header.correlation_id,
            client_host = request_context.conn.client_ip,
        )
    )]
    pub async fn handle_find_coordinator_request(
        request_context: &mut RequestContext<'_>,
        request: FindCoordinatorRequest,
    ) -> AppResult<()> {
        let response = request_context
            .group_coordinator
            .find_coordinator(request)
            .await?;
        response
            .write_to(
                &mut request_context.conn.writer,
                &request_context.request_header.api_version,
                request_context.request_header.correlation_id,
            )
            .await?;
        debug!("find coordinator response write to client");
        Ok(())
    }
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            client_id = request_context.request_header.client_id,
            correlation_id = request_context.request_header.correlation_id,
            client_host = request_context.conn.client_ip,
        )
    )]
    pub async fn handle_join_group_request(
        request_context: &mut RequestContext<'_>,
        mut request: JoinGroupRequest,
    ) -> AppResult<()> {
        if let Some(client_id) = request_context.request_header.client_id.clone() {
            request.client_id = client_id;
        }
        let join_result = request_context
            .group_coordinator
            .clone()
            .handle_join_group(request, request_context)
            .await
            .unwrap();

        debug!("join group result");
        let error_code = match join_result.error {
            Some(error) => error as i16,
            None => 0,
        };
        let members = join_result.members.unwrap_or_default();
        let join_group_response = JoinGroupResponse::new(
            error_code,
            join_result.generation_id,
            join_result.sub_protocol,
            join_result.member_id,
            join_result.leader_id,
            members,
        );
        debug!("join group response");
        join_group_response
            .write_to(
                &mut request_context.conn.writer,
                &request_context.request_header.api_version,
                request_context.request_header.correlation_id,
            )
            .await?;
        Ok(())
    }
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            client_id = request_context.request_header.client_id,
            correlation_id = request_context.request_header.correlation_id,
            client_host = request_context.conn.client_ip,
        )
    )]
    pub async fn handle_sync_group_request(
        request_context: &mut RequestContext<'_>,
        request: SyncGroupRequest,
    ) -> AppResult<()> {
        debug!("sync group request: {:?}", request);
        // request 中的bytesmut 来自于connection中的buffer，不能破坏掉，需要返还给connection，这里将BytesMut转换成Bytes，返还BytesMut
        let group_assignment = request
            .group_assignment
            .iter()
            .map(|(k, v)| (k.clone(), Bytes::from(v.clone())))
            .collect();
        let sync_group_result = request_context
            .group_coordinator
            .clone()
            .handle_sync_group(
                &request.group_id,
                &request.member_id,
                request.generation_id,
                group_assignment,
                request_context,
            )
            .await;
        debug!("sync group response: {:?}", sync_group_result);
        match sync_group_result {
            Ok(response) => {
                response
                    .write_to(
                        &mut request_context.conn.writer,
                        &request_context.request_header.api_version,
                        request_context.request_header.correlation_id,
                    )
                    .await?;
                Ok(())
            }
            Err(e) => {
                let error_code = ErrorCode::from(&e);
                SyncGroupResponse::new(error_code, 0, Bytes::new())
                    .write_to(
                        &mut request_context.conn.writer,
                        &request_context.request_header.api_version,
                        request_context.request_header.correlation_id,
                    )
                    .await?;
                Ok(())
            }
        }
    }
    pub async fn handle_leave_group_request(
        request_context: &mut RequestContext<'_>,
        request: LeaveGroupRequest,
    ) -> AppResult<()> {
        todo!()
    }
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            client_id = request_context.request_header.client_id,
            correlation_id = request_context.request_header.correlation_id,
            client_host = request_context.conn.client_ip,
        )
    )]
    pub async fn handle_heartbeat_request(
        request_context: &mut RequestContext<'_>,
        request: HeartbeatRequest,
    ) -> AppResult<()> {
        debug!("received heartbeat request");
        let result = request_context
            .group_coordinator
            .clone()
            .handle_heartbeat(&request.group_id, &request.member_id, request.generation_id)
            .await;
        let response = match result {
            Ok(_) => HeartbeatResponse::new(KafkaError::None, 0),
            Err(e) => HeartbeatResponse::new(e, 0),
        };

        response
            .write_to(
                &mut request_context.conn.writer,
                &request_context.request_header.api_version,
                request_context.request_header.correlation_id,
            )
            .await?;
        // debug!("finished heartbeat response ");
        Ok(())
    }
    #[instrument(
        level = "trace",
        skip_all,
        fields(
            client_id = request_context.request_header.client_id,
            correlation_id = request_context.request_header.correlation_id,
            client_host = request_context.conn.client_ip,
        )
    )]
    pub async fn handle_offset_commit_request(
        request_context: &mut RequestContext<'_>,
        request: OffsetCommitRequest,
    ) -> AppResult<()> {
        debug!("offset commit request: {:?}", request);
        let result = request_context
            .group_coordinator
            .clone()
            .handle_commit_offsets(
                &request.group_id,
                &request.member_id,
                request.generation_id,
                request.offset_data,
            )
            .await;

        debug!("offset commit result: {:?}", result);
        let response = OffsetCommitResponse::new(0, result);
        response
            .write_to(
                &mut request_context.conn.writer,
                &request_context.request_header.api_version,
                request_context.request_header.correlation_id,
            )
            .await?;
        Ok(())
    }
    #[instrument(
        level = "trace",
        skip_all,
        fields(
            client_id = request_context.request_header.client_id,
            correlation_id = request_context.request_header.correlation_id,
            client_host = request_context.conn.client_ip,
        )
    )]
    pub async fn handle_fetch_offsets_request(
        request_context: &mut RequestContext<'_>,
        request: FetchOffsetsRequest,
    ) -> AppResult<()> {
        let result = request_context
            .group_coordinator
            .clone()
            .handle_fetch_offsets(&request.group_id, request.partitions);
        debug!("fetch offsets result: {:?}", result);
        if let Ok(offsets) = result {
            let response = FetchOffsetsResponse::new(KafkaError::None, offsets);
            response
                .write_to(
                    &mut request_context.conn.writer,
                    &request_context.request_header.api_version,
                    request_context.request_header.correlation_id,
                )
                .await?;
        } else {
            FetchOffsetsResponse::new(result.err().unwrap(), HashMap::new())
                .write_to(
                    &mut request_context.conn.writer,
                    &request_context.request_header.api_version,
                    request_context.request_header.correlation_id,
                )
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn respond_invalid_request(
        error: AppError,
        request_context: &RequestContext<'_>,
    ) -> AppResult<()> {
        error!(
            "Invalid request with api key: {:?}, correlation id: {}",
            request_context.request_header.api_key, request_context.request_header.correlation_id
        );
        debug!("respond invalid request");
        Ok(())
    }
}
