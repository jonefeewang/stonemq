use std::io;
use std::io::ErrorKind;

use bytes::{Buf, Bytes, BytesMut};

use crate::{AppError, AppResult};
use crate::AppError::Incomplete;
use crate::config::DynamicConfig;
use crate::protocol::{ApiKey, ProtocolCodec};
use crate::request::{ApiRequest, RequestHeader};
use crate::request::produce::ProduceRequest;

impl TryFrom<&RequestFrame> for ApiRequest {
    type Error = AppError;

    fn try_from(frame: &RequestFrame) -> Result<Self, Self::Error> {
        match frame.request_header.api_key {
            ApiKey::Produce(_, _) => {
                let mut body = frame.body.clone();
                let produce_request =
                    ProduceRequest::read_from(&mut body, &frame.request_header.api_version)?;
                return Ok(ApiRequest::Produce(produce_request));
            }
            ApiKey::Metadata(_, _) => {
                todo!()
            }
        }
    }
}

/// 来自客户端的请求Frame
///
#[derive(Debug)]
pub struct RequestFrame {
    pub request_header: RequestHeader,
    pub body: BytesMut,
}
/// 返回给客户端的Response Frame
#[derive(Debug)]
pub struct ResponseFrame {
    correlation_id: i32,
    body: Bytes,
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
        return match RequestFrame::check(buffer, dynamic_config) {
            Ok(_) => {
                let length_bytes = buffer.get(0..4).unwrap();
                let body_length = i32::from_be_bytes(length_bytes.try_into().unwrap());
                //这里必须使用BytesMut, 因为后续在验证record batch时，需要assign offset,修改缓冲区里的内容
                let mut body = buffer.split_to(body_length as usize + 4);
                let request_header = RequestHeader::read_from(&mut body)?;
                let frame = RequestFrame {
                    request_header,
                    body,
                };
                Ok(Some(frame))
            }
            Err(AppError::Incomplete) => Ok(None),
            Err(e) => Err(e),
        };
    }
}
