use crate::AppError::{NetworkReadError, NetworkWriteError};
use crate::AppResult;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::borrow::Cow;

pub struct BufferUtils {}

impl BufferUtils {
    pub fn safe_read_i8(buffer: &mut Bytes) -> AppResult<i8> {
        if buffer.remaining() < 1 {
            return Err(NetworkReadError(Cow::Borrowed(
                "Insufficient buffer size for i8.",
            )));
        }
        Ok(buffer.get_i8())
    }
    pub fn safe_read_i16(buffer: &mut Bytes) -> AppResult<i16> {
        if buffer.remaining() < 2 {
            return Err(NetworkReadError(Cow::Borrowed(
                "Insufficient buffer size for i16.",
            )));
        }
        Ok(buffer.get_i16())
    }
    pub fn safe_read_i32(buffer: &mut Bytes) -> AppResult<i32> {
        if buffer.remaining() < 4 {
            return Err(NetworkReadError(Cow::Borrowed(
                "Insufficient buffer size for i32.",
            )));
        }
        Ok(buffer.get_i32())
    }

    pub fn can_write_nbytes(buffer: &mut BytesMut, bytes: usize) -> AppResult<()> {
        if buffer.remaining_mut() < bytes {
            return Err(NetworkWriteError(Cow::Owned(format!(
                "Insufficient buffer size for {}.",
                bytes
            ))));
        }
        Ok(())
    }
    pub fn can_read_nbytes(buffer: &mut Bytes, bytes: usize) -> AppResult<()> {
        if buffer.remaining() < bytes {
            return Err(NetworkReadError(Cow::Owned(format!(
                "not enough bytes remains. need{},but only {}.",
                bytes,
                buffer.remaining()
            ))));
        }
        Ok(())
    }
}
