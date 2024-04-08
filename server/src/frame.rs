use std::io;
use std::io::{Cursor, ErrorKind};

use bytes::{Buf, Bytes, BytesMut};

use crate::{AppError, AppResult};
use crate::AppError::{Incomplete, InvalidValue, MalformedProtocolEncoding};
use crate::config::DynamicConfig;
use crate::protocol::{Acks, ApiKey, ApiVersion};
use crate::protocol::ApiKey::{Metadata, Produce};
use crate::protocol::ApiVersion::{V0, V1, V2, V3};
use crate::request::RequestEnum;

impl TryFrom<RequestFrame> for RequestEnum {
    type Error = AppError;

    fn try_from(mut frame: RequestFrame) -> Result<Self, Self::Error> {
        match frame.api_key {
            Produce => match frame.api_version {
                V0 | V1 | V2 => {
                    let frame_body = &mut frame.body;
                    let mut acks: Acks = Acks::All(-1);
                    if frame_body.len() >= 2 {
                        acks = frame_body.get_i16().try_into()?;
                    } else {
                        return Err(MalformedProtocolEncoding("<acks> field"));
                    }
                    let mut timeout = 0;
                    if frame_body.len() >= 4 {
                        timeout = frame_body.get_i32();
                    } else {
                        return Err(MalformedProtocolEncoding("<timeout> field"));
                    }
                    let topic_count = frame_body.get_i32();
                }
                V3 => {}
            },
            Metadata => {}
        }
        todo!()
    }
}
impl TryFrom<i16> for ApiVersion {
    type Error = AppError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(V0),
            1 => Ok(V1),
            invalid => Err(InvalidValue("api version", invalid.to_string())),
        }
    }
}

/// 来自客户端的请求Frame
///
#[derive(Debug)]
pub struct RequestFrame {
    len: u32,
    api_key: ApiKey,
    api_version: ApiVersion,
    correlation_id: i32,
    body: Bytes,
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
    pub fn check(
        cursor: &mut Cursor<&mut BytesMut>,
        dynamic_config: &DynamicConfig,
    ) -> AppResult<()> {
        if cursor.remaining() < 4 {
            return Err(Incomplete);
        }
        let body_length = cursor.get_i32();

        if body_length < 0 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("frame size {} less than 0", body_length),
            )
            .into());
        }
        if body_length > dynamic_config.max_package_size() as i32 {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("Frame of length {} is too large.", body_length),
            )
            .into());
        }
        if cursor.remaining() < body_length as usize {
            cursor.get_mut().reserve(body_length as usize);
            return Err(Incomplete);
        }
        Ok(())
    }
    /// 解析一个Request Frame
    /// 注意：这通常是在check之后进行
    /// 返回：解析出的Frame
    ///
    pub(crate) fn parse(
        cursor: &mut Cursor<&mut BytesMut>,
        dynamic_config: &DynamicConfig,
    ) -> AppResult<Option<RequestFrame>> {
        // perform a check to ensure we have enough data
        return match RequestFrame::check(cursor, dynamic_config) {
            Ok(_) => {
                // reset cursor position
                cursor.set_position(0);
                let length = cursor.get_u32();
                let api_key = cursor.get_i16().try_into()?;
                let api_version = cursor.get_i16().try_into()?;

                let correlation_id = cursor.get_i32();
                let body = Bytes::copy_from_slice(cursor.chunk());

                let frame = RequestFrame {
                    len: length,
                    api_key,
                    api_version,
                    correlation_id,
                    body,
                };
                // discard read frames
                cursor.get_mut().advance(4 + length as usize);
                Ok(Some(frame))
            }
            Err(AppError::Incomplete) => Ok(None),
            Err(e) => Err(e),
        };
    }
}
