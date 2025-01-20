//! Frame Processing Implementation
//!
//! This module implements the frame processing functionality for the network protocol.
//! It handles parsing, validation, and processing of network frames that encapsulate
//! client requests.
//!
//! # Frame Format
//!
//! Each frame consists of:
//! - 4-byte length prefix (big-endian)
//! - Request header
//! - Request body
//!
//! # Features
//!
//! - Frame size validation
//! - Partial frame handling
//! - Buffer management
//! - Error handling for malformed frames
//! - Configurable size limits

use bytes::{Buf, BytesMut};

use crate::request::RequestHeader;
use crate::AppError::Incomplete;
use crate::{global_config, AppError, AppResult};

/// Represents a request frame in the network protocol.
///
/// A frame contains both the request header and body, providing
/// a complete encapsulation of a client request.
///
/// # Fields
///
/// * `request_header` - Header containing request metadata
/// * `request_body` - Body containing the actual request data
#[derive(Debug)]
pub struct RequestFrame {
    pub request_header: RequestHeader,
    pub request_body: BytesMut,
}

impl RequestFrame {
    /// Checks if a buffer contains a complete frame.
    ///
    /// Validates:
    /// - Buffer has enough bytes for length prefix
    /// - Frame size is positive
    /// - Frame size is within configured limits
    /// - Buffer contains complete frame data
    ///
    /// # Arguments
    ///
    /// * `buffer` - Buffer containing frame data
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Buffer contains a valid complete frame
    /// * `Err(Incomplete)` - Buffer contains a partial frame
    /// * `Err(e)` - Frame is invalid
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

    /// Attempts to parse a complete frame from a buffer.
    ///
    /// If the buffer contains a complete frame, consumes the frame data
    /// and returns the parsed frame. Otherwise, returns None or an error.
    ///
    /// # Arguments
    ///
    /// * `buffer` - Buffer containing frame data
    ///
    /// # Returns
    ///
    /// * `Ok(Some(frame))` - Successfully parsed a complete frame
    /// * `Ok(None)` - Buffer contains a partial frame
    /// * `Err(e)` - Error parsing frame
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
