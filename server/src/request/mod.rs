use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tracing::{debug, error, trace};

use crate::{AppError, AppResult, Connection, ReplicaManager};
use crate::AppError::IllegalStateError;
use crate::broker::Node;
use crate::protocol::{ApiKey, ApiVersion, ProtocolCodec};
use crate::protocol::api_schemas::SUPPORTED_API_VERSIONS;
use crate::request::metadata::{MetaDataRequest, MetadataResponse};
use crate::request::produce::{ProduceRequest, ProduceResponse};

mod create_topic;

pub mod metadata;
pub mod produce;

#[derive(Debug)]
pub enum ApiRequest {
    Produce(ProduceRequest),
    Metadata(MetaDataRequest),
    ApiVersion(ApiVersionRequest),
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
                let size = 2 + 2 + 4 + 4 + client_id_size;
                size
            }
            None => {
                let size = 2 + 2 + 4 + 4;
                size
            }
        }
    }
}

#[derive(Debug)]
pub struct RequestContext<'c> {
    pub conn: &'c mut Connection,
    pub request_header: RequestHeader,
    pub replica_manager: Arc<ReplicaManager>,
}
impl<'c> RequestContext<'c> {
    pub fn new(
        conn: &'c mut Connection,
        request_header: RequestHeader,
        replica_manager: Arc<ReplicaManager>,
    ) -> Self {
        RequestContext {
            conn,
            request_header,
            replica_manager,
        }
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
                Self::handle_produce_request(request_context, request).await
            }

            ApiRequest::Metadata(request) => {
                Self::handle_metadata_request(request_context, request).await
            }
            ApiRequest::ApiVersion(request) => {
                Self::handle_api_version_request(request_context, request).await
            }
        }
    }

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
    pub async fn handle_api_version_request(
        request_context: &mut RequestContext<'_>,
        request: ApiVersionRequest,
    ) -> AppResult<()> {
        let response = request.process()?;
        trace!("api versions response: {:?}", response);
        response
            .write_to(
                &mut request_context.conn.writer,
                &request_context.request_header.api_version,
                request_context.request_header.correlation_id,
            )
            .await?;
        Ok(())
    }

    pub async fn handle_metadata_request(
        request_context: &mut RequestContext<'_>,
        request: MetaDataRequest,
    ) -> AppResult<()> {
        // send metadata for specific topics

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
    pub(crate) async fn respond_invalid_request(
        error: AppError,
        request_context: &RequestContext<'_>,
    ) -> AppResult<()> {
        error!(
            "Invalid request: {:?} with request context:{:?}",
            error, &request_context
        );
        todo!()
    }
}
