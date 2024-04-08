use std::io::{self, Cursor, ErrorKind};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, BufWriter};
use tokio::net::TcpStream;

use crate::AppResult;
use crate::config::DynamicConfig;
use crate::frame::RequestFrame;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    dynamic_config: DynamicConfig,
}

impl Connection {
    pub fn new(socket: TcpStream, dynamic_config: DynamicConfig) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
            dynamic_config,
        }
    }
    /// 读取frame
    pub async fn read_frame(&mut self) -> AppResult<Option<RequestFrame>> {
        loop {
            let mut cursor = Cursor::new(&mut self.buffer);
            // 如果遇到数据格式错误，或包超出大小，就退出，关闭connection
            if let Some(frame) = RequestFrame::parse(&mut cursor, &self.dynamic_config)? {
                return Ok(Some(frame));
            }
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
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
