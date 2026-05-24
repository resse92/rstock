use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use arrow_array::builder::{Float64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use clap::Args;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::task::JoinSet;

use crate::api::ApiClient;
use crate::models::{MarketRequest, MinuteBar1m, DEFAULT_TIMEOUT_SECS};
use crate::normalize::normalize_full_kline_response;
use crate::s3::{
    build_s3_client, ensure_bucket, upload_local_file, validate_parquet_file,
    write_parquet_bytes_local, S3Settings,
};
use crate::tdx_source;
use crate::utils::{chunked, load_stock_codes_from_file};

#[derive(Debug, Args, Clone)]
pub struct SyncMinuteArgs {
    #[arg(long, help = "开始日期，YYYY-MM-DD 或 YYYYMMDD")]
    pub start_date: String,

    #[arg(long, help = "结束日期，YYYY-MM-DD 或 YYYYMMDD")]
    pub end_date: String,

    #[arg(long, default_value_t = 100)]
    pub chunk_size: usize,

    #[arg(long, default_value_t = 4)]
    pub fetch_concurrency: usize,

    #[arg(long)]
    pub stock_codes_file: Option<PathBuf>,

    #[arg(long, env = "QMT_API_HOST", default_value = "http://127.0.0.1:8000")]
    pub base_url: String,

    #[arg(long, env = "QMT_API_AUTHORIZATION")]
    pub authorization: Option<String>,

    #[arg(long, env = "QMT_API_TIMEOUT", default_value_t = DEFAULT_TIMEOUT_SECS)]
    pub timeout: u64,

    #[arg(long, env = "S3_BUCKET", default_value = "stock")]
    pub s3_bucket: String,

    #[arg(long, env = "LOCAL_STAGING_DIR", default_value = "data/staging")]
    pub staging_dir: PathBuf,

    #[arg(long, env = "S3_REGION", default_value = "us-east-1")]
    pub s3_region: String,

    #[arg(long, env = "S3_ACCESS_KEY")]
    pub s3_access_key: Option<String>,

    #[arg(long, env = "S3_SECRET_KEY")]
    pub s3_secret_key: Option<String>,

    #[arg(long, help = "S3 endpoint，默认读取 S3_HOST 或 s3_host")]
    pub s3_host: Option<String>,
}

pub async fn run_sync_minute(args: SyncMinuteArgs) -> Result<()> {
    if args.chunk_size == 0 {
        return Err(anyhow!("--chunk-size 必须大于 0"));
    }
    if args.fetch_concurrency == 0 {
        return Err(anyhow!("--fetch-concurrency 必须大于 0"));
    }

    let start_date = compact_date(&args.start_date)?;
    let end_date = compact_date(&args.end_date)?;
    let s3_host = args
        .s3_host
        .or_else(|| env_var_any(&["S3_HOST", "s3_host"]))
        .ok_or_else(|| anyhow!("缺少 S3 host，请在 .env 设置 s3_host 或 S3_HOST"))?;

    let api = ApiClient::new(
        args.base_url,
        args.authorization,
        Duration::from_secs(args.timeout),
    )?;
    let stock_codes = if let Some(path) = args.stock_codes_file.as_ref() {
        load_stock_codes_from_file(path)?
    } else {
        api.discover_all_stock_codes().await?
    };

    let s3_settings = S3Settings {
        endpoint: s3_host,
        bucket: args.s3_bucket,
        access_key: args.s3_access_key,
        secret_key: args.s3_secret_key,
        region: args.s3_region,
    };
    let s3 = build_s3_client(&s3_settings).await?;
    ensure_bucket(&s3, &s3_settings.bucket).await?;

    let batches = chunked(&stock_codes, args.chunk_size);
    let mut grouped: BTreeMap<(String, String), Vec<MinuteBar1m>> = BTreeMap::new();
    let mut join_set = JoinSet::new();
    let mut next_batch_idx = 0usize;

    while next_batch_idx < batches.len() || !join_set.is_empty() {
        while next_batch_idx < batches.len() && join_set.len() < args.fetch_concurrency {
            let chunk = batches[next_batch_idx].clone();
            let api_clone = api.clone();
            let start = start_date.clone();
            let end = end_date.clone();
            join_set.spawn(async move {
                let req =
                    MarketRequest::new(chunk.clone(), "1m", start.clone(), end.clone(), "none");
                let rows = match api_clone.fetch_market_batch(&req).await {
                    Ok(resp) => normalize_full_kline_response(&resp, "1m"),
                    Err(err) => {
                        eprintln!("[QMT][minute] 批次失败，切换 TDX 兜底: {err:#}");
                        Vec::new()
                    }
                };
                let mut bars = rows
                    .iter()
                    .filter_map(MinuteBar1m::from_normalized)
                    .collect::<Vec<_>>();
                let found: BTreeSet<String> = bars.iter().map(|bar| bar.symbol.clone()).collect();
                let missing = chunk
                    .iter()
                    .filter(|code| !found.contains(*code))
                    .cloned()
                    .collect::<Vec<_>>();
                if !missing.is_empty() {
                    eprintln!(
                        "[QMT][minute] 批次缺少 {} 只股票，切换 TDX 兜底",
                        missing.len()
                    );
                    bars.extend(tdx_source::fetch_minute_bars(&missing, &start, &end)?);
                }
                Ok::<Vec<MinuteBar1m>, anyhow::Error>(bars)
            });
            next_batch_idx += 1;
        }

        let bars = join_set
            .join_next()
            .await
            .ok_or_else(|| anyhow!("fetch 并发任务异常结束"))?
            .map_err(|e| anyhow!("fetch task join error: {e}"))??;
        for bar in bars {
            if bar.time.len() < 10 {
                continue;
            }
            let trade_date = bar.time[0..10].to_string();
            grouped
                .entry((trade_date, bar.exchange.clone()))
                .or_default()
                .push(bar);
        }
    }

    let mut uploaded = 0usize;
    let mut rows = 0usize;
    for ((trade_date, exchange), mut bars) in grouped {
        bars.sort_by(|a, b| a.symbol.cmp(&b.symbol).then(a.time.cmp(&b.time)));
        let key = format!(
            "curated/minute_bars_1m/trade_date={trade_date}/exchange={exchange}/part-000.parquet"
        );
        let local_path = args.staging_dir.join(&key);
        write_parquet_bytes_local(&local_path, minute_to_parquet_bytes(&bars)?)?;
        validate_parquet_file(&local_path)?;
        upload_local_file(&s3, &s3_settings.bucket, &key, &local_path).await?;
        rows += bars.len();
        uploaded += 1;
        println!("[PUT] {key} rows={}", bars.len());
    }

    println!(
        "[DONE] 分钟线上传完成: {} 条, {} 个分区文件, bucket={}",
        rows, uploaded, s3_settings.bucket
    );
    Ok(())
}

fn minute_to_parquet_bytes(rows: &[MinuteBar1m]) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("time", DataType::Utf8, false),
        Field::new("open", DataType::Float64, true),
        Field::new("high", DataType::Float64, true),
        Field::new("low", DataType::Float64, true),
        Field::new("close", DataType::Float64, true),
        Field::new("volume", DataType::Float64, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("factor", DataType::Float64, true),
        Field::new("settle", DataType::Float64, true),
        Field::new("openInterest", DataType::Float64, true),
    ]));

    let mut symbol = StringBuilder::new();
    let mut exchange = StringBuilder::new();
    let mut time = StringBuilder::new();
    let mut open = Float64Builder::new();
    let mut high = Float64Builder::new();
    let mut low = Float64Builder::new();
    let mut close = Float64Builder::new();
    let mut volume = Float64Builder::new();
    let mut amount = Float64Builder::new();
    let mut factor = Float64Builder::new();
    let mut settle = Float64Builder::new();
    let mut open_interest = Float64Builder::new();

    for row in rows {
        symbol.append_value(&row.symbol);
        exchange.append_value(&row.exchange);
        time.append_value(&row.time);
        open.append_option(row.open);
        high.append_option(row.high);
        low.append_option(row.low);
        close.append_option(row.close);
        volume.append_option(row.volume);
        amount.append_option(row.amount);
        factor.append_option(row.adj_factor);
        settle.append_option(row.settle);
        open_interest.append_option(row.open_interest);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(symbol.finish()) as ArrayRef,
            Arc::new(exchange.finish()) as ArrayRef,
            Arc::new(time.finish()) as ArrayRef,
            Arc::new(open.finish()) as ArrayRef,
            Arc::new(high.finish()) as ArrayRef,
            Arc::new(low.finish()) as ArrayRef,
            Arc::new(close.finish()) as ArrayRef,
            Arc::new(volume.finish()) as ArrayRef,
            Arc::new(amount.finish()) as ArrayRef,
            Arc::new(factor.finish()) as ArrayRef,
            Arc::new(settle.finish()) as ArrayRef,
            Arc::new(open_interest.finish()) as ArrayRef,
        ],
    )?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(128 * 1024)
        .build();
    let mut cursor = Cursor::new(Vec::new());
    let mut writer = ArrowWriter::try_new(&mut cursor, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(cursor.into_inner())
}

fn compact_date(input: &str) -> Result<String> {
    let s = input.trim();
    if s.len() == 8 && s.chars().all(|c| c.is_ascii_digit()) {
        return Ok(s.to_string());
    }
    if s.len() == 10 && s.as_bytes().get(4) == Some(&b'-') && s.as_bytes().get(7) == Some(&b'-') {
        let out = s.replace('-', "");
        if out.len() == 8 && out.chars().all(|c| c.is_ascii_digit()) {
            return Ok(out);
        }
    }
    Err(anyhow!(
        "无效日期格式: {input}，应为 YYYY-MM-DD 或 YYYYMMDD"
    ))
}

fn env_var_any(keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Ok(v) = env::var(key) {
            if !v.trim().is_empty() {
                return Some(v);
            }
        }
    }
    None
}
