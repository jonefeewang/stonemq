use std::io;
use std::io::{Cursor, ErrorKind};

use bytes::{Buf, Bytes, BytesMut};

use crate::{AppError, AppResult};
use crate::AppError::Incomplete;
use crate::config::DynamicConfig;

/// 来自客户端的请求Frame
///
#[derive(Debug)]
pub struct RequestFrame {
    len: u32,
    api_key: i16,
    api_version: i16,
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
                let api_key = cursor.get_i16();
                let api_version = cursor.get_i16();
                let correlation_id = cursor.get_i32();
                let body = Bytes::copy_from_slice(cursor.chunk());

                let frame = RequestFrame {
                    len: length,
                    api_key,
                    api_version,
                    correlation_id,
                    body,
                };
                //
                cursor.get_mut().advance(4 + length as usize);
                Ok(Some(frame))
            }
            Err(AppError::Incomplete) => Ok(None),
            Err(e) => Err(e),
        };
    }
}
