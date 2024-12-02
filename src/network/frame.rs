use std::io;
use std::io::ErrorKind;

use bytes::{Buf, BytesMut};

use crate::protocol::{ApiKey, ProtocolCodec};
use crate::request::consumer_group::{
    FetchOffsetsRequest, FindCoordinatorRequest, HeartbeatRequest, JoinGroupRequest,
    LeaveGroupRequest, OffsetCommitRequest, SyncGroupRequest,
};
use crate::request::fetch::FetchRequest;
use crate::request::metadata::MetaDataRequest;
use crate::request::produce::ProduceRequest;
use crate::request::{ApiRequest, ApiVersionRequest, RequestHeader};
use crate::AppError::Incomplete;
use crate::DynamicConfig;
use crate::{AppError, AppResult};

impl TryFrom<(BytesMut, &RequestHeader)> for ApiRequest {
    type Error = AppError;

    fn try_from(
        (mut request_body, request_header): (BytesMut, &RequestHeader),
    ) -> Result<Self, Self::Error> {
        match request_header.api_key {
            ApiKey::Produce => {
                let produce_request =
                    ProduceRequest::decode(&mut request_body, &request_header.api_version)?;
                Ok(ApiRequest::Produce(produce_request))
            }
            ApiKey::Fetch => {
                let fetch_request =
                    FetchRequest::decode(&mut request_body, &request_header.api_version)?;
                Ok(ApiRequest::Fetch(fetch_request))
            }
            ApiKey::Metadata => {
                let metadata_request =
                    MetaDataRequest::decode(&mut request_body, &request_header.api_version)?;
                Ok(ApiRequest::Metadata(metadata_request))
            }
            ApiKey::ApiVersionKey => {
                let api_version_request =
                    ApiVersionRequest::decode(&mut request_body, &request_header.api_version)?;
                Ok(ApiRequest::ApiVersion(api_version_request))
            }
            ApiKey::JoinGroup => {
                let join_group_request =
                    JoinGroupRequest::decode(&mut request_body, &request_header.api_version)?;
                Ok(ApiRequest::JoinGroup(join_group_request))
            }
            ApiKey::SyncGroup => {
                let sync_group_request =
                    SyncGroupRequest::decode(&mut request_body, &request_header.api_version)?;
                Ok(ApiRequest::SyncGroup(sync_group_request))
            }
            ApiKey::LeaveGroup => {
                let leave_group_request =
                    LeaveGroupRequest::decode(&mut request_body, &request_header.api_version)?;
                Ok(ApiRequest::LeaveGroup(leave_group_request))
            }
            ApiKey::Heartbeat => {
                let heartbeat_request =
                    HeartbeatRequest::decode(&mut request_body, &request_header.api_version)?;
                Ok(ApiRequest::Heartbeat(heartbeat_request))
            }
            ApiKey::OffsetCommit => {
                let offset_commit_request =
                    OffsetCommitRequest::decode(&mut request_body, &request_header.api_version)?;
                Ok(ApiRequest::OffsetCommit(offset_commit_request))
            }
            ApiKey::OffsetFetch => {
                let fetch_offsets_request =
                    FetchOffsetsRequest::decode(&mut request_body, &request_header.api_version)?;
                Ok(ApiRequest::FetchOffsets(fetch_offsets_request))
            }
            ApiKey::FindCoordinator => {
                let find_coordinator_request =
                    FindCoordinatorRequest::decode(&mut request_body, &request_header.api_version)?;
                Ok(ApiRequest::FindCoordinator(find_coordinator_request))
            }
        }
    }
}

/// 来自客户端的请求Frame
///
#[derive(Debug)]
pub struct RequestFrame {
    pub request_header: RequestHeader,
    pub request_body: BytesMut,
}
/// 返回给客户端的Response Frame
#[derive(Debug)]
pub struct ResponseFrame {
    correlation_id: i32,
    body: BytesMut,
}

impl RequestFrame {
    /// 检查一下当前buffer内是否够一个完整的frame
    /// 返回：
    /// 如果数据不够的话(需要继续从socket内读取)返回Err(Incomplete),数据格式错误、或数据包超过配置的大小
    /// 都会返回Err(InvalidData)。
    /// 如果数据足够的话，返回()
    pub fn check(buffer: &mut BytesMut, dynamic_config: &DynamicConfig) -> AppResult<()> {
        if buffer.remaining() < 4 {
            return Err(Incomplete);
        }
        let bytes_slice = buffer.get(0..4).unwrap();
        let body_size = i32::from_be_bytes(bytes_slice.try_into().unwrap());
        if body_size < 0 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("frame size {} less than 0", body_size),
            )
            .into());
        }
        if body_size > dynamic_config.max_package_size() as i32 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("Frame of length {} is too large.", body_size),
            )
            .into());
        }
        if buffer.remaining() < body_size as usize + 4 {
            buffer.reserve(body_size as usize + 4);
            return Err(Incomplete);
        }
        Ok(())
    }
    /// 解析一个Request Frame
    /// 注意：这通常是在check之后进行
    /// 返回：解析出的Frame
    ///
    pub(crate) fn parse(
        buffer: &mut BytesMut,
        dynamic_config: &DynamicConfig,
    ) -> AppResult<Option<RequestFrame>> {
        // perform a check to ensure we have enough data
        match RequestFrame::check(buffer, dynamic_config) {
            Ok(_) => {
                // let length_bytes = buffer.get(0..4).ok_or(Incomplete)?;
                // let body_length = i32::from_be_bytes(length_bytes.try_into().or(Err(Incomplete))?);
                let body_length = buffer.get_i32();
                //这里必须使用BytesMut, 因为后续在验证record batch时，需要assign offset,修改缓冲区里的内容
                let mut body = buffer.split_to(body_length as usize);
                let request_header = RequestHeader::read_from(&mut body)?;
                let frame = RequestFrame {
                    request_header,
                    request_body: body,
                };
                Ok(Some(frame))
            }
            Err(AppError::Incomplete) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
