// Copyright 2025 jonefeewang@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Connection Management Implementation
//!
//! This module implements the TCP connection management functionality,
//! providing abstractions for handling individual client connections,
//! reading frames, and managing connection state.
//!
//! # Features
//!
//! - Asynchronous read operations
//! - Buffer management for network I/O
//! - Connection state tracking
//! - Client identification
//! - Error handling for connection issues

use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio::net::tcp::OwnedReadHalf;

use crate::network::RequestFrame;
use crate::AppError;
use crate::AppResult;

/// Represents a TCP connection to a client.
///
/// Manages the reading of frames from a TCP connection and maintains
/// connection state information.
///
/// # Fields
///
/// * `reader` - Buffered reader for the TCP connection
/// * `buffer` - Buffer for storing incoming data
/// * `client_ip` - IP address of the connected client
#[derive(Debug)]
pub struct Connection {
    pub reader: BufReader<OwnedReadHalf>,
    pub buffer: BytesMut,
    pub client_ip: String,
}

impl Connection {
    /// Creates a new Connection instance.
    ///
    /// # Arguments
    ///
    /// * `reader` - The read half of a TCP connection
    /// * `buffer_size` - Initial size of the read buffer
    ///
    /// # Returns
    ///
    /// A new Connection instance configured with the specified parameters
    pub fn new(reader: OwnedReadHalf, buffer_size: usize) -> Connection {
        let peer_addr = reader.peer_addr().unwrap();
        let client_ip = peer_addr.ip().to_string();
        Connection {
            reader: BufReader::new(reader),
            buffer: BytesMut::with_capacity(buffer_size),
            client_ip,
        }
    }

    /// Reads a frame from the connection.
    ///
    /// Continuously reads from the connection until either a complete frame
    /// is received or an error occurs. Handles partial reads and connection
    /// closure gracefully.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(frame))` - A complete frame was read
    /// * `Ok(None)` - Connection was closed cleanly
    /// * `Err(e)` - An error occurred during reading
    pub async fn read_frame(&mut self) -> AppResult<Option<RequestFrame>> {
        loop {
            if let Some(frame) = RequestFrame::parse(&mut self.buffer)? {
                return Ok(Some(frame));
            }
            if 0 == self
                .reader
                .read_buf(&mut self.buffer)
                .await
                .map_err(|e| AppError::DetailedIoError(format!("read frame error: {}", e)))?
            {
                return if self.buffer.is_empty() {
                    Ok(None)
                } else {
                    Err(AppError::DetailedIoError(
                        "client close the connection while sending a frame".to_string(),
                    ))
                };
            }
        }
    }
}
