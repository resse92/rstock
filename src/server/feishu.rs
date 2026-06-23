use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::Serialize;
use tracing::{error, info};

const FEISHU_BOT_WEBHOOK_BASE: &str = "https://open.feishu.cn/open-apis/bot/v2/hook";

#[derive(Debug, Clone)]
pub struct FeishuNotifier {
    token: Option<String>,
    client: Client,
    webhook_base: String,
}

impl FeishuNotifier {
    pub fn new(token: Option<String>) -> Self {
        Self::with_webhook_base(token, FEISHU_BOT_WEBHOOK_BASE.to_string())
    }

    fn with_webhook_base(token: Option<String>, webhook_base: String) -> Self {
        Self {
            token,
            client: Client::new(),
            webhook_base,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.token.is_some()
    }

    pub async fn send_post(&self, post: FeishuPostMessage) -> Result<()> {
        let Some(token) = self.token.as_deref() else {
            info!(
                target: "rstock::feishu",
                message_title = %post.title,
                line_count = post.lines.len(),
                "skip feishu post because bot token is not configured"
            );
            return Ok(());
        };

        let url = format!("{}/{token}", self.webhook_base.trim_end_matches('/'));
        let title = post.title.clone();
        let line_count = post.lines.len();
        info!(
            target: "rstock::feishu",
            message_title = %title,
            line_count,
            "send feishu post start"
        );
        let response = self
            .client
            .post(url)
            .json(&FeishuPostWebhookMessage::new(post))
            .send()
            .await
            .map_err(|err| {
                error!(
                    target: "rstock::feishu",
                    message_title = %title,
                    line_count,
                    error = %err,
                    "send feishu post request failed"
                );
                err
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            error!(
                target: "rstock::feishu",
                message_title = %title,
                line_count,
                status = %status,
                body = %body,
                "send feishu post failed"
            );
            return Err(anyhow!(
                "feishu webhook failed: status={status}, body={body}"
            ));
        }

        info!(
            target: "rstock::feishu",
            message_title = %title,
            line_count,
            "send feishu post succeeded"
        );
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FeishuPostMessage {
    title: String,
    lines: Vec<String>,
}

impl FeishuPostMessage {
    pub fn new(title: impl Into<String>, lines: Vec<String>) -> Self {
        Self {
            title: title.into(),
            lines,
        }
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn line_count(&self) -> usize {
        self.lines.len()
    }
}

#[derive(Debug, Serialize)]
struct FeishuPostWebhookMessage {
    msg_type: &'static str,
    content: FeishuPostWebhookContent,
}

impl FeishuPostWebhookMessage {
    fn new(post: FeishuPostMessage) -> Self {
        let content = post
            .lines
            .into_iter()
            .map(|line| vec![FeishuPostTag::text(line)])
            .collect::<Vec<_>>();
        Self {
            msg_type: "post",
            content: FeishuPostWebhookContent {
                post: FeishuPostBody {
                    zh_cn: FeishuPostLocale {
                        title: post.title,
                        content,
                    },
                },
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct FeishuPostWebhookContent {
    post: FeishuPostBody,
}

#[derive(Debug, Serialize)]
struct FeishuPostBody {
    zh_cn: FeishuPostLocale,
}

#[derive(Debug, Serialize)]
struct FeishuPostLocale {
    title: String,
    content: Vec<Vec<FeishuPostTag>>,
}

#[derive(Debug, Serialize)]
struct FeishuPostTag {
    tag: &'static str,
    text: String,
}

impl FeishuPostTag {
    fn text(text: String) -> Self {
        Self { tag: "text", text }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use serde_json::Value;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn send_post_noops_when_token_missing() -> Result<()> {
        let notifier = FeishuNotifier::new(None);
        notifier
            .send_post(FeishuPostMessage::new("scan", vec!["hello".to_string()]))
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn send_post_posts_expected_payload() -> Result<()> {
        let received = Arc::new(Mutex::new(None));
        let server = spawn_test_server(200, "ok", received.clone()).await?;
        let notifier = FeishuNotifier::with_webhook_base(
            Some("test-token".to_string()),
            format!("http://{}", server.addr),
        );

        notifier
            .send_post(FeishuPostMessage::new(
                "scan result",
                vec!["hello feishu".to_string()],
            ))
            .await?;

        let request = received
            .lock()
            .await
            .clone()
            .expect("request should be captured");
        assert!(request.starts_with("POST /test-token HTTP/1.1"));
        assert!(request.contains("content-type: application/json"));
        let body = request
            .split("\r\n\r\n")
            .nth(1)
            .expect("request body should exist");
        let payload: Value = serde_json::from_str(body)?;
        assert_eq!(payload["msg_type"], "post");
        assert_eq!(payload["content"]["post"]["zh_cn"]["title"], "scan result");
        assert_eq!(
            payload["content"]["post"]["zh_cn"]["content"][0][0]["text"],
            "hello feishu"
        );

        server.handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn send_post_preserves_multiple_lines() -> Result<()> {
        let received = Arc::new(Mutex::new(None));
        let server = spawn_test_server(200, "ok", received.clone()).await?;
        let notifier = FeishuNotifier::with_webhook_base(
            Some("post-token".to_string()),
            format!("http://{}", server.addr),
        );

        notifier
            .send_post(FeishuPostMessage::new(
                "scan result",
                vec!["line one".to_string(), "line two".to_string()],
            ))
            .await?;

        let request = received
            .lock()
            .await
            .clone()
            .expect("request should be captured");
        let body = request
            .split("\r\n\r\n")
            .nth(1)
            .expect("request body should exist");
        let payload: Value = serde_json::from_str(body)?;
        assert_eq!(payload["msg_type"], "post");
        assert_eq!(payload["content"]["post"]["zh_cn"]["title"], "scan result");
        assert_eq!(
            payload["content"]["post"]["zh_cn"]["content"][0][0]["text"],
            "line one"
        );
        assert_eq!(
            payload["content"]["post"]["zh_cn"]["content"][1][0]["text"],
            "line two"
        );

        server.handle.await??;
        Ok(())
    }

    #[tokio::test]
    async fn send_post_returns_error_on_non_success_status() -> Result<()> {
        let server = spawn_test_server(500, "boom", Arc::new(Mutex::new(None))).await?;
        let notifier = FeishuNotifier::with_webhook_base(
            Some("bad-token".to_string()),
            format!("http://{}", server.addr),
        );

        let err = notifier
            .send_post(FeishuPostMessage::new("scan", vec!["hello".to_string()]))
            .await
            .expect_err("should fail");
        let message = format!("{err:#}");
        assert!(message.contains("status=500"));
        assert!(message.contains("body=boom"));

        server.handle.await??;
        Ok(())
    }

    struct TestServer {
        addr: std::net::SocketAddr,
        handle: tokio::task::JoinHandle<Result<()>>,
    }

    async fn spawn_test_server(
        status: u16,
        body: &'static str,
        received: Arc<Mutex<Option<String>>>,
    ) -> Result<TestServer> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            let mut buffer = vec![0u8; 4096];
            let bytes_read = stream.read(&mut buffer).await?;
            *received.lock().await =
                Some(String::from_utf8_lossy(&buffer[..bytes_read]).to_string());
            let response = format!(
                "HTTP/1.1 {status} TEST\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await?;
            Ok(())
        });

        Ok(TestServer { addr, handle })
    }
}
