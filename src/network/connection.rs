use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio::net::tcp::OwnedReadHalf;

use crate::network::RequestFrame;
use crate::AppError;
use crate::AppResult;

/// Represents a connection to a client.
///
/// This struct encapsulates a TCP stream and a buffer for reading data from the stream.
/// It also holds a reference to the dynamic configuration of the server.
#[derive(Debug)]
pub struct Connection {
    pub reader: BufReader<OwnedReadHalf>,
    pub buffer: BytesMut,
    pub client_ip: String,
}

impl Connection {
    /// Creates a new `Connection` from a `TcpStream` and a `DynamicConfig`.
    ///
    /// The `TcpStream` is wrapped in a `BufWriter` for efficient writing, and a `BytesMut` buffer
    /// is created with an initial capacity of 4KB for reading data from the stream.
    pub fn new(reader: OwnedReadHalf) -> Connection {
        let peer_addr = reader.peer_addr().unwrap();
        let client_ip = peer_addr.ip().to_string();
        Connection {
            reader: BufReader::new(reader),
            buffer: BytesMut::with_capacity(4 * 1024),
            client_ip,
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
                    // client has closed the connection gracefully
                    Ok(None)
                } else {
                    // client close the connection while sending a frame

                    Err(AppError::DetailedIoError(
                        "client close the connection while sending a frame".to_string(),
                    ))
                };
            }
        }
    }
}
