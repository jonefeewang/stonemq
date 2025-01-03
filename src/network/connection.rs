use bytes::BytesMut;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio::net::tcp::OwnedReadHalf;

use crate::network::RequestFrame;
use crate::AppError;
use crate::AppResult;

#[derive(Debug)]
pub struct Connection {
    pub reader: BufReader<OwnedReadHalf>,
    pub buffer: BytesMut,
    pub client_ip: String,
}

impl Connection {
    pub fn new(reader: OwnedReadHalf, buffer_size: usize) -> Connection {
        let peer_addr = reader.peer_addr().unwrap();
        let client_ip = peer_addr.ip().to_string();
        Connection {
            reader: BufReader::new(reader),
            buffer: BytesMut::with_capacity(buffer_size),
            client_ip,
        }
    }
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
