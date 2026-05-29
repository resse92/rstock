use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use minio::s3::client::{Client, ClientBuilder};
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::segmented_bytes::SegmentedBytes;
use minio::s3::types::S3Api;
use parquet::file::reader::SerializedFileReader;

#[derive(Debug, Clone)]
pub struct S3Settings {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub region: String,
}

pub type S3Client = Client;

pub async fn build_s3_client(settings: &S3Settings) -> Result<S3Client> {
    let mut endpoint = settings.endpoint.clone();
    if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
        endpoint = format!("http://{endpoint}");
    }

    let base_url: BaseUrl = endpoint.parse()?;

    let provider = match (&settings.access_key, &settings.secret_key) {
        (Some(ak), Some(sk)) => Some(Box::new(StaticProvider::new(ak, sk, None)) as Box<_>),
        _ => None,
    };

    let client = ClientBuilder::new(base_url).provider(provider).build()?;

    Ok(client)
}

pub async fn ensure_bucket(s3: &S3Client, bucket: &str) -> Result<()> {
    let exists_resp = s3.bucket_exists(bucket).send().await?;
    if exists_resp.exists {
        return Ok(());
    }
    s3.create_bucket(bucket).send().await?;
    Ok(())
}

pub fn write_parquet_bytes_local(path: &Path, parquet_bytes: Vec<u8>) -> Result<PathBuf> {
    if parquet_bytes.is_empty() {
        return Err(anyhow!("empty parquet bytes"));
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, parquet_bytes)?;
    Ok(path.to_path_buf())
}

pub fn validate_parquet_file(path: &Path) -> Result<()> {
    let meta = fs::metadata(path)?;
    if meta.len() == 0 {
        return Err(anyhow!("empty parquet file: {}", path.display()));
    }
    let file = fs::File::open(path)?;
    let _reader = SerializedFileReader::new(file)?;
    Ok(())
}

pub async fn upload_local_file(
    s3: &S3Client,
    bucket: &str,
    key: &str,
    local_path: &Path,
) -> Result<()> {
    let bytes = fs::read(local_path)?;
    if bytes.is_empty() {
        return Err(anyhow!(
            "refuse to upload empty file: {}",
            local_path.display()
        ));
    }
    let body = SegmentedBytes::from(Bytes::from(bytes));
    s3.put_object(bucket, key, body).send().await?;
    Ok(())
}
