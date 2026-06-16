use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::Serialize;

const FEISHU_BOT_WEBHOOK_BASE: &str = "https://open.feishu.cn/open-apis/bot/v2/hook";

#[derive(Debug, Clone)]
pub struct FeishuNotifier {
    token: Option<String>,
    client: Client,
}

impl FeishuNotifier {
    pub fn new(token: Option<String>) -> Self {
        Self {
            token,
            client: Client::new(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.token.is_some()
    }

    pub async fn send_text(&self, text: impl Into<String>) -> Result<()> {
        let Some(token) = self.token.as_deref() else {
            return Ok(());
        };

        let url = format!("{FEISHU_BOT_WEBHOOK_BASE}/{token}");
        let response = self
            .client
            .post(url)
            .json(&FeishuTextMessage::new(text.into()))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!(
                "feishu webhook failed: status={status}, body={body}"
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct FeishuTextMessage {
    msg_type: &'static str,
    content: FeishuTextContent,
}

impl FeishuTextMessage {
    fn new(text: String) -> Self {
        Self {
            msg_type: "text",
            content: FeishuTextContent { text },
        }
    }
}

#[derive(Debug, Serialize)]
struct FeishuTextContent {
    text: String,
}
