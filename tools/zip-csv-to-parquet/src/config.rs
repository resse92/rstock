use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct MinuteS3Config {
    pub host: String,
    pub bucket: String,
    pub region: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub staging_dir: PathBuf,
    pub part_size: usize,
    pub upload_concurrency: usize,
    pub manifest_file: PathBuf,
}

pub fn load_minute_s3_config(path: impl AsRef<Path>) -> Result<MinuteS3Config> {
    let path = path.as_ref();
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("读取配置文件失败: {}", path.display()))?;
    let config: RootConfig =
        toml::from_str(&raw).with_context(|| format!("解析 TOML 配置失败: {}", path.display()))?;

    Ok(MinuteS3Config {
        host: config.s3.host,
        bucket: config.s3.bucket,
        region: config.s3.region,
        access_key: empty_to_none(config.s3.access_key),
        secret_key: empty_to_none(config.s3.secret_key),
        staging_dir: config.s3.local_staging_dir,
        part_size: config.tools.minute_s3.part_size,
        upload_concurrency: config.tools.minute_s3.upload_concurrency,
        manifest_file: config.tools.minute_s3.manifest_file,
    })
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
    s3: S3Section,
    #[serde(default)]
    tools: ToolsSection,
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
struct ToolsSection {
    #[serde(default)]
    minute_s3: MinuteS3Section,
}

impl Default for ToolsSection {
    fn default() -> Self {
        Self {
            minute_s3: MinuteS3Section::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct MinuteS3Section {
    #[serde(default = "default_part_size")]
    part_size: usize,
    #[serde(default = "default_upload_concurrency")]
    upload_concurrency: usize,
    #[serde(default = "default_manifest_file")]
    manifest_file: PathBuf,
}

impl Default for MinuteS3Section {
    fn default() -> Self {
        Self {
            part_size: default_part_size(),
            upload_concurrency: default_upload_concurrency(),
            manifest_file: default_manifest_file(),
        }
    }
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

fn default_part_size() -> usize {
    200_000
}

fn default_upload_concurrency() -> usize {
    4
}

fn default_manifest_file() -> PathBuf {
    PathBuf::from("meta/ingestion/minute_zip_manifest.txt")
}
