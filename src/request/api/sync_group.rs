use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use tracing::debug;

use crate::request::kafka_errors::ErrorCode;
use crate::request::RequestContext;

use super::ApiHandler;

pub struct SyncGroupRequestHandler;
impl ApiHandler for SyncGroupRequestHandler {
    type Request = SyncGroupRequest;
    type Response = SyncGroupResponse;

    async fn handle_request(
        &self,
        request: SyncGroupRequest,
        context: &RequestContext,
    ) -> SyncGroupResponse {
        debug!("sync group request: {:?}", request);
        // request 中的bytesmut 来自于connection中的buffer，不能破坏掉，需要返还给connection，这里将BytesMut转换成Bytes，返还BytesMut
        let group_assignment = request
            .group_assignment
            .iter()
            .map(|(k, v)| (k.clone(), Bytes::from(v.clone())))
            .collect();
        let sync_group_result = context
            .group_coordinator
            .clone()
            .handle_sync_group(
                &request.group_id,
                &request.member_id,
                request.generation_id,
                group_assignment,
            )
            .await;
        debug!("sync group response: {:?}", sync_group_result);
        match sync_group_result {
            Ok(response) => response,
            Err(e) => {
                let error_code = ErrorCode::from(&e);
                SyncGroupResponse::new(error_code, 0, Bytes::new())
            }
        }
    }
}

#[derive(Debug)]
pub struct SyncGroupRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_assignment: HashMap<String, BytesMut>,
}

#[derive(Debug)]
pub struct SyncGroupResponse {
    pub error_code: ErrorCode,
    pub throttle_time_ms: i32,
    pub member_assignment: Bytes,
}
impl SyncGroupResponse {
    pub fn new(error_code: ErrorCode, throttle_time_ms: i32, member_assignment: Bytes) -> Self {
        SyncGroupResponse {
            error_code,
            throttle_time_ms,
            member_assignment,
        }
    }
}
