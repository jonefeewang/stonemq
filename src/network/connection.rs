use std::io::{self, ErrorKind};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, BufWriter};
use tokio::net::TcpStream;

use crate::AppResult;
use crate::DynamicConfig;
use crate::network::RequestFrame;

/// Represents a connection to a client.
///
/// This struct encapsulates a TCP stream and a buffer for reading data from the stream.
/// It also holds a reference to the dynamic configuration of the server.
#[derive(Debug)]
pub struct Connection {
    pub writer: BufWriter<TcpStream>,
    pub buffer: BytesMut,
    pub compression_buffer: BytesMut,
    dynamic_config: DynamicConfig,
}

impl Connection {
    /// Creates a new `Connection` from a `TcpStream` and a `DynamicConfig`.
    ///
    /// The `TcpStream` is wrapped in a `BufWriter` for efficient writing, and a `BytesMut` buffer
    /// is created with an initial capacity of 4KB for reading data from the stream.
    pub fn new(socket: TcpStream, dynamic_config: DynamicConfig) -> Connection {
        Connection {
            writer: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
            compression_buffer: BytesMut::with_capacity(4 * 1024),
            dynamic_config,
        }
    }
    /// Reads a `RequestFrame` from the connection.
    ///
    /// This method continuously reads data from the stream into the buffer until a complete
    /// `RequestFrame` can be parsed. If a data format error is encountered, or if the packet
    /// exceeds the size limit, an error is returned and the connection should be closed.
    ///
    /// If the client closes the connection while a frame is being sent, an error is returned.
    /// If the client closes the connection gracefully, `None` is returned.
    pub async fn read_frame(&mut self) -> AppResult<Option<RequestFrame>> {
        loop {
            // If a data format error is encountered, or if the packet exceeds the size limit,
            // terminate the process and close the connection.
            if let Some(frame) = RequestFrame::parse(&mut self.buffer, &self.dynamic_config)? {
                return Ok(Some(frame));
            }
            if 0 == self.writer.read_buf(&mut self.buffer).await? {
                return if self.buffer.is_empty() {
                    // client has closed the connection gracefully
                    Ok(None)
                } else {
                    // client close the connection while sending a frame
                    Err(
                        io::Error::new(ErrorKind::ConnectionReset, "connection reset by peer")
                            .into(),
                    )
                };
            }
        }
    }
}
