use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio::net::TcpListener;
use tokio::stream::StreamExt; // 用于Stream的扩展方法，如next()
use tokio::io::AsyncReadExt; // 用于异步读取

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 监听TCP连接
    let listener = TcpListener::bind("127.0.0.1:12345").await?;

    // 接受连接
    let (socket, _) = listener.accept().await?;

    // 将TCP流包装在Framed中，使用LengthDelimitedCodec
    let mut framed = Framed::new(socket, LengthDelimitedCodec::new());

    // 读取流中的消息
    while let Some(frame) = framed.next().await {
        match frame {
            Ok(bytes) => {
                // 成功接收到一个完整的帧
                let msg = String::from_utf8(bytes.to_vec())?;
                println!("Received: {}", msg);
            }
            Err(e) => {
                // 处理可能的错误
                eprintln!("Error receiving frame: {}", e);
                break;
            }
        }
    }

    Ok(())
}
