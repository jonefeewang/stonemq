use bytes::{Buf, BytesMut};

use crate::request::RequestHeader;
use crate::AppError::Incomplete;
use crate::{global_config, AppError, AppResult};

#[derive(Debug)]
pub struct RequestFrame {
    pub request_header: RequestHeader,
    pub request_body: BytesMut,
}

impl RequestFrame {
    pub fn check(buffer: &mut BytesMut) -> AppResult<()> {
        if buffer.remaining() < 4 {
            return Err(Incomplete);
        }
        let bytes_slice = buffer.get(0..4).unwrap();
        let body_size = i32::from_be_bytes(bytes_slice.try_into().unwrap());
        if body_size < 0 {
            return Err(AppError::DetailedIoError(format!(
                "frame size {} less than 0",
                body_size
            )));
        }
        if body_size > global_config().network.max_package_size as i32 {
            return Err(AppError::DetailedIoError(format!(
                "Frame of length {} is too large.",
                body_size
            )));
        }
        if buffer.remaining() < body_size as usize + 4 {
            buffer.reserve(body_size as usize + 4);
            return Err(Incomplete);
        }
        Ok(())
    }

    pub(crate) fn parse(buffer: &mut BytesMut) -> AppResult<Option<RequestFrame>> {
        // perform a check to ensure we have enough data
        match RequestFrame::check(buffer) {
            Ok(_) => {
                let body_length = buffer.get_i32();
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
