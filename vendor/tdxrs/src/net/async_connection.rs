use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::{Result, TdxError};

/// 异步 TCP 连接
pub struct AsyncTcpConnection {
    stream: TcpStream,
}

impl AsyncTcpConnection {
    pub async fn connect(ip: &str, port: u16, timeout_secs: f64) -> Result<Self> {
        let addr = format!("{}:{}", ip, port);
        let stream = tokio::time::timeout(
            std::time::Duration::from_secs_f64(timeout_secs),
            TcpStream::connect(&addr),
        )
        .await
        .map_err(|_| TdxError::ConnectionTimeout)?
        .map_err(|e| TdxError::Connection(format!("Failed to connect to {}: {}", addr, e)))?;

        stream
            .set_nodelay(true)
            .map_err(|e| TdxError::Connection(format!("set_nodelay: {}", e)))?;

        Ok(Self { stream })
    }

    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.stream
            .write_all(data)
            .await
            .map_err(|e| TdxError::Connection(format!("send failed: {}", e)))?;
        Ok(())
    }

    pub async fn recv(&mut self, len: usize) -> Result<Vec<u8>> {
        let mut buf = vec![0u8; len];
        let mut total = 0;
        while total < len {
            let n = self
                .stream
                .read(&mut buf[total..])
                .await
                .map_err(|e| TdxError::Connection(format!("recv failed: {}", e)))?;
            if n == 0 {
                return Err(TdxError::Disconnected);
            }
            total += n;
        }
        Ok(buf)
    }

    pub fn close(&mut self) {
        // tokio TcpStream 的 shutdown 不接受参数
        // 使用 into_std 转换后关闭
        use tokio::io::AsyncWriteExt;
        let _ = self.stream.shutdown();
    }

    pub fn is_open(&self) -> bool {
        self.stream.peer_addr().is_ok()
    }
}
