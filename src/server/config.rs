use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use jobs::models::DEFAULT_QMT_API_HOST;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: SocketAddr,
    pub daily_cron: String,
    pub minute_cron: String,
    pub daily_chunk_size: usize,
    pub minute_chunk_size: usize,
    pub daily_fetch_concurrency: usize,
    pub minute_fetch_concurrency: usize,
    pub daily_stock_codes_file: Option<PathBuf>,
    pub minute_stock_codes_file: Option<PathBuf>,
    pub base_url: String,
    pub authorization: Option<String>,
    pub timeout: u64,
    pub s3_bucket: String,
    pub staging_dir: PathBuf,
    pub s3_region: String,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub s3_host: Option<String>,
}

impl ServerConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("读取配置文件失败: {}", path.display()))?;
        let config: RootConfig = toml::from_str(&raw)
            .with_context(|| format!("解析 TOML 配置失败: {}", path.display()))?;

        Ok(Self {
            bind: config
                .server
                .bind
                .parse()
                .context("server.bind 格式错误")?,
            daily_cron: config.server.daily_cron,
            minute_cron: config.server.minute_cron,
            daily_chunk_size: config.sync.daily.chunk_size,
            minute_chunk_size: config.sync.minute.chunk_size,
            daily_fetch_concurrency: config.sync.daily.fetch_concurrency,
            minute_fetch_concurrency: config.sync.minute.fetch_concurrency,
            daily_stock_codes_file: config.sync.daily.stock_codes_file,
            minute_stock_codes_file: config.sync.minute.stock_codes_file,
            base_url: config.qmt.host,
            authorization: config.qmt.authorization,
            timeout: config.qmt.timeout,
            s3_bucket: config.s3.bucket,
            staging_dir: config.s3.local_staging_dir,
            s3_region: config.s3.region,
            s3_access_key: empty_to_none(config.s3.access_key),
            s3_secret_key: empty_to_none(config.s3.secret_key),
            s3_host: Some(config.s3.host),
        })
    }
}

fn empty_to_none(value: Option<String>) -> Option<String> {
    value.and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

#[derive(Debug, Deserialize)]
struct RootConfig {
    #[serde(default)]
    server: ServerSection,
    #[serde(default)]
    qmt: QmtSection,
    #[serde(default)]
    s3: S3Section,
    #[serde(default)]
    sync: SyncSection,
}

#[derive(Debug, Deserialize)]
struct ServerSection {
    #[serde(default = "default_bind")]
    bind: String,
    #[serde(default = "default_daily_cron")]
    daily_cron: String,
    #[serde(default = "default_minute_cron")]
    minute_cron: String,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            daily_cron: default_daily_cron(),
            minute_cron: default_minute_cron(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct QmtSection {
    #[serde(default = "default_qmt_host")]
    host: String,
    authorization: Option<String>,
    #[serde(default = "default_qmt_timeout")]
    timeout: u64,
}

impl Default for QmtSection {
    fn default() -> Self {
        Self {
            host: default_qmt_host(),
            authorization: None,
            timeout: default_qmt_timeout(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct S3Section {
    #[serde(default = "default_s3_host")]
    host: String,
    #[serde(default = "default_s3_bucket")]
    bucket: String,
    #[serde(default = "default_s3_region")]
    region: String,
    access_key: Option<String>,
    secret_key: Option<String>,
    #[serde(default = "default_staging_dir")]
    local_staging_dir: PathBuf,
}

impl Default for S3Section {
    fn default() -> Self {
        Self {
            host: default_s3_host(),
            bucket: default_s3_bucket(),
            region: default_s3_region(),
            access_key: None,
            secret_key: None,
            local_staging_dir: default_staging_dir(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct SyncSection {
    #[serde(default)]
    daily: DailySyncSection,
    #[serde(default)]
    minute: MinuteSyncSection,
}

impl Default for SyncSection {
    fn default() -> Self {
        Self {
            daily: DailySyncSection::default(),
            minute: MinuteSyncSection::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct DailySyncSection {
    #[serde(default = "default_daily_chunk_size")]
    chunk_size: usize,
    #[serde(default = "default_daily_fetch_concurrency")]
    fetch_concurrency: usize,
    stock_codes_file: Option<PathBuf>,
}

impl Default for DailySyncSection {
    fn default() -> Self {
        Self {
            chunk_size: default_daily_chunk_size(),
            fetch_concurrency: default_daily_fetch_concurrency(),
            stock_codes_file: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct MinuteSyncSection {
    #[serde(default = "default_minute_chunk_size")]
    chunk_size: usize,
    #[serde(default = "default_minute_fetch_concurrency")]
    fetch_concurrency: usize,
    stock_codes_file: Option<PathBuf>,
}

impl Default for MinuteSyncSection {
    fn default() -> Self {
        Self {
            chunk_size: default_minute_chunk_size(),
            fetch_concurrency: default_minute_fetch_concurrency(),
            stock_codes_file: None,
        }
    }
}

fn default_bind() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_daily_cron() -> String {
    "0 30 15 * * *".to_string()
}

fn default_minute_cron() -> String {
    "0 10 15 * * *".to_string()
}

fn default_qmt_host() -> String {
    DEFAULT_QMT_API_HOST.to_string()
}

fn default_qmt_timeout() -> u64 {
    30
}

fn default_s3_host() -> String {
    "127.0.0.1:9000".to_string()
}

fn default_s3_bucket() -> String {
    "stock".to_string()
}

fn default_s3_region() -> String {
    "us-east-1".to_string()
}

fn default_staging_dir() -> PathBuf {
    PathBuf::from("data/staging")
}

fn default_daily_chunk_size() -> usize {
    200
}

fn default_daily_fetch_concurrency() -> usize {
    8
}

fn default_minute_chunk_size() -> usize {
    100
}

fn default_minute_fetch_concurrency() -> usize {
    4
}
