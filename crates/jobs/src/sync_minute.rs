use std::collections::{BTreeMap, BTreeSet};
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
use polars::prelude::{Column, DataFrame, IdxCa};
use tokio::task::JoinSet;

use crate::api::ApiClient;
use crate::kline_frame::{concat_frames, frame_symbols, partition_frame_by_trade_date_exchange};
use crate::models::{MarketRequest, DEFAULT_QMT_API_HOST, DEFAULT_TIMEOUT_SECS};
use crate::tdx_source;
use crate::utils::{chunked, load_stock_codes_from_file};
use storage::s3::{
    build_s3_client, ensure_bucket, upload_local_file, validate_parquet_file,
    write_parquet_bytes_local, S3Settings,
};

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

    #[arg(long, default_value = DEFAULT_QMT_API_HOST)]
    pub base_url: String,

    #[arg(long)]
    pub authorization: Option<String>,

    #[arg(long, default_value_t = DEFAULT_TIMEOUT_SECS)]
    pub timeout: u64,

    #[arg(long, default_value = "stock")]
    pub s3_bucket: String,

    #[arg(long, default_value = "data/staging")]
    pub staging_dir: PathBuf,

    #[arg(long, default_value = "us-east-1")]
    pub s3_region: String,

    #[arg(long)]
    pub s3_access_key: Option<String>,

    #[arg(long)]
    pub s3_secret_key: Option<String>,

    #[arg(long, help = "S3 endpoint")]
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
        .ok_or_else(|| anyhow!("缺少 S3 host，请在配置中提供"))?;

    let api = ApiClient::new(
        args.base_url,
        args.authorization,
        Duration::from_secs(args.timeout),
    )?;
    let stock_codes = load_sync_stock_codes(&api, args.stock_codes_file.as_ref()).await?;

    let grouped = fetch_minute_grouped_bars(
        &api,
        &stock_codes,
        &start_date,
        &end_date,
        args.chunk_size,
        args.fetch_concurrency,
    )
    .await?;

    let s3_settings = S3Settings {
        endpoint: s3_host,
        bucket: args.s3_bucket,
        access_key: args.s3_access_key,
        secret_key: args.s3_secret_key,
        region: args.s3_region,
    };
    let s3 = build_s3_client(&s3_settings).await?;
    ensure_bucket(&s3, &s3_settings.bucket).await?;

    let mut uploaded = 0usize;
    let mut rows = 0usize;
    for ((trade_date, exchange), frame) in grouped {
        let key = minute_partition_key(&trade_date, &exchange);
        let local_path =
            write_minute_partition_file_local(&args.staging_dir, &trade_date, &exchange, &frame)?;
        validate_parquet_file(&local_path)?;
        upload_local_file(&s3, &s3_settings.bucket, &key, &local_path).await?;
        rows += frame.height();
        uploaded += 1;
        println!("[PUT] {key} rows={}", frame.height());
    }

    println!(
        "[DONE] 分钟线上传完成: {} 条, {} 个分区文件, bucket={}",
        rows, uploaded, s3_settings.bucket
    );
    Ok(())
}

async fn load_sync_stock_codes(
    api: &ApiClient,
    stock_codes_file: Option<&PathBuf>,
) -> Result<Vec<String>> {
    if let Some(path) = stock_codes_file {
        load_stock_codes_from_file(path)
    } else {
        api.discover_all_stock_codes().await
    }
}

async fn fetch_minute_grouped_bars(
    api: &ApiClient,
    stock_codes: &[String],
    start_date: &str,
    end_date: &str,
    chunk_size: usize,
    fetch_concurrency: usize,
) -> Result<BTreeMap<(String, String), DataFrame>> {
    let start_compact = compact_date(start_date)?;
    let end_compact = compact_date(end_date)?;
    let start_api = minute_start_time(&start_compact);
    let end_api = minute_end_time(&end_compact);
    let batches = chunked(stock_codes, chunk_size);
    let mut grouped: BTreeMap<(String, String), DataFrame> = BTreeMap::new();
    let mut join_set = JoinSet::new();
    let mut next_batch_idx = 0usize;

    while next_batch_idx < batches.len() || !join_set.is_empty() {
        while next_batch_idx < batches.len() && join_set.len() < fetch_concurrency {
            let chunk = batches[next_batch_idx].clone();
            let api_clone = api.clone();
            let start = start_compact.clone();
            let end = end_compact.clone();
            let start_api = start_api.clone();
            let end_api = end_api.clone();
            join_set.spawn(async move {
                let req = MarketRequest::new(
                    chunk.clone(),
                    "1m",
                    start_api.clone(),
                    end_api.clone(),
                    "none",
                );
                let mut frames = Vec::new();
                match api_clone.fetch_kline_frame(&req).await {
                    Ok(frame) => frames.push(frame),
                    Err(err) => {
                        eprintln!("[QMT][minute] 批次失败，切换 TDX 兜底: {err:#}");
                        for code in &chunk {
                            let single_req = MarketRequest::new(
                                vec![code.clone()],
                                "1m",
                                start_api.clone(),
                                end_api.clone(),
                                "none",
                            );
                            match api_clone.fetch_kline_frame(&single_req).await {
                                Ok(frame) => frames.push(frame),
                                Err(single_err) => {
                                    eprintln!(
                                        "[QMT][minute] 单票 {} 请求失败，留给 TDX 兜底: {single_err:#}",
                                        code
                                    );
                                }
                            }
                        }
                    }
                };
                let found = if frames.is_empty() {
                    BTreeSet::new()
                } else {
                    let merged = concat_frames(frames.clone())?;
                    frame_symbols(&merged)?
                };
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
                    frames.push(tdx_source::fetch_minute_bars_frame(&missing, &start, &end)?);
                }
                let merged = concat_frames(frames)?;
                Ok::<DataFrame, anyhow::Error>(merged)
            });
            next_batch_idx += 1;
        }

        let frame = join_set
            .join_next()
            .await
            .ok_or_else(|| anyhow!("fetch 并发任务异常结束"))?
            .map_err(|e| anyhow!("fetch task join error: {e}"))??;
        for (key, partition) in partition_frame_by_trade_date_exchange(&frame)? {
            if let Some(existing) = grouped.get_mut(&key) {
                existing.vstack_mut(&partition)?;
            } else {
                grouped.insert(key, partition);
            }
        }
    }

    Ok(grouped)
}

fn minute_partition_key(trade_date: &str, exchange: &str) -> String {
    format!("curated/minute_bars_1m/trade_date={trade_date}/exchange={exchange}/part-000.parquet")
}

fn write_minute_partition_file_local(
    staging_dir: &std::path::Path,
    trade_date: &str,
    exchange: &str,
    frame: &DataFrame,
) -> Result<PathBuf> {
    let key = minute_partition_key(trade_date, exchange);
    let local_path = staging_dir.join(key);
    write_parquet_bytes_local(&local_path, minute_to_parquet_bytes(frame)?)
}

fn minute_to_parquet_bytes(frame: &DataFrame) -> Result<Vec<u8>> {
    let frame = sort_frame_by_columns(frame, &["symbol", "time"])?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("trade_date", DataType::Utf8, false),
        Field::new("time", DataType::Utf8, false),
        Field::new("open", DataType::Float64, true),
        Field::new("high", DataType::Float64, true),
        Field::new("low", DataType::Float64, true),
        Field::new("close", DataType::Float64, true),
        Field::new("volume", DataType::Float64, true),
        Field::new("turn_over", DataType::Float64, true),
        Field::new("turn_over_rate", DataType::Float64, true),
        Field::new("factor", DataType::Float64, true),
    ]));

    let mut symbol = StringBuilder::new();
    let mut exchange = StringBuilder::new();
    let mut trade_date = StringBuilder::new();
    let mut time = StringBuilder::new();
    let mut open = Float64Builder::new();
    let mut high = Float64Builder::new();
    let mut low = Float64Builder::new();
    let mut close = Float64Builder::new();
    let mut volume = Float64Builder::new();
    let mut turn_over = Float64Builder::new();
    let mut turn_over_rate = Float64Builder::new();
    let mut factor = Float64Builder::new();

    let symbols = frame.column("symbol")?.str()?;
    let exchanges = frame.column("exchange")?.str()?;
    let trade_dates = frame.column("trade_date")?.str()?;
    let times = frame.column("time")?.str()?;
    let open_values = frame.column("open")?.f64()?;
    let high_values = frame.column("high")?.f64()?;
    let low_values = frame.column("low")?.f64()?;
    let close_values = frame.column("close")?.f64()?;
    let volume_values = frame.column("volume")?.f64()?;
    let amount_values = frame.column("amount")?.f64()?;
    let turnover_rate_values = frame.column("turnover_rate")?.f64()?;
    let factor_values = frame.column("factor")?.f64()?;

    for idx in 0..frame.height() {
        let Some(symbol_value) = symbols.get(idx) else {
            continue;
        };
        let Some(exchange_value) = exchanges.get(idx) else {
            continue;
        };
        let Some(trade_date_value) = trade_dates.get(idx) else {
            continue;
        };
        let Some(time_value) = times.get(idx) else {
            continue;
        };
        symbol.append_value(symbol_value);
        exchange.append_value(exchange_value);
        trade_date.append_value(trade_date_value);
        time.append_value(time_value);
        open.append_option(open_values.get(idx));
        high.append_option(high_values.get(idx));
        low.append_option(low_values.get(idx));
        close.append_option(close_values.get(idx));
        volume.append_option(volume_values.get(idx));
        turn_over.append_option(amount_values.get(idx));
        turn_over_rate.append_option(turnover_rate_values.get(idx));
        factor.append_option(factor_values.get(idx));
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(symbol.finish()) as ArrayRef,
            Arc::new(exchange.finish()) as ArrayRef,
            Arc::new(trade_date.finish()) as ArrayRef,
            Arc::new(time.finish()) as ArrayRef,
            Arc::new(open.finish()) as ArrayRef,
            Arc::new(high.finish()) as ArrayRef,
            Arc::new(low.finish()) as ArrayRef,
            Arc::new(close.finish()) as ArrayRef,
            Arc::new(volume.finish()) as ArrayRef,
            Arc::new(turn_over.finish()) as ArrayRef,
            Arc::new(turn_over_rate.finish()) as ArrayRef,
            Arc::new(factor.finish()) as ArrayRef,
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

fn sort_frame_by_columns(frame: &DataFrame, columns: &[&str]) -> Result<DataFrame> {
    let mut order = (0..frame.height()).collect::<Vec<_>>();
    order.sort_by(|&left, &right| compare_frame_rows(frame, left, right, columns));
    let idx = IdxCa::from_vec(
        "idx".into(),
        order.into_iter().map(|idx| idx as u32).collect(),
    );
    frame.take(&idx).map_err(Into::into)
}

fn compare_frame_rows(
    frame: &DataFrame,
    left: usize,
    right: usize,
    columns: &[&str],
) -> std::cmp::Ordering {
    for column in columns {
        let Some(series) = frame
            .get_columns()
            .iter()
            .find(|item| item.name().as_str() == *column)
        else {
            continue;
        };
        let ordering = match series {
            Column::Series(s) => compare_series_values(s, left, right),
            _ => std::cmp::Ordering::Equal,
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

fn compare_series_values(
    series: &polars::prelude::Series,
    left: usize,
    right: usize,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match series.dtype() {
        polars::prelude::DataType::String => {
            let ca = series.str().ok();
            ca.and_then(|values| Some(values.get(left).cmp(&values.get(right))))
                .unwrap_or(Ordering::Equal)
        }
        polars::prelude::DataType::Float64 => {
            let ca = series.f64().ok();
            ca.and_then(|values| values.get(left).partial_cmp(&values.get(right)))
                .unwrap_or(Ordering::Equal)
        }
        _ => Ordering::Equal,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn minute_trade_date(time: &str) -> &str {
    time.get(0..10).unwrap_or(time)
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

fn minute_start_time(compact_date: &str) -> String {
    format!("{compact_date}091500")
}

fn minute_end_time(compact_date: &str) -> String {
    format!("{compact_date}150000")
}

#[cfg(test)]
mod tests {
    use super::{
        fetch_minute_grouped_bars, minute_partition_key, minute_trade_date,
        write_minute_partition_file_local,
    };
    use crate::api::ApiClient;
    use crate::kline_frame::{minute_bars_from_frame, minute_bars_to_frame};
    use crate::models::MinuteBar1m;
    use anyhow::Result;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::RowAccessor;
    use serde::Deserialize;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn writes_all_minute_rows_for_single_trade_date_to_local_staging() -> Result<()> {
        let staging_dir = temp_dir("sync-minute");
        let bars = vec![
            minute_bar("000001.SZ", "SZ", "2026-05-24 09:31:00", 10.0),
            minute_bar("000001.SZ", "SZ", "2026-05-24 09:32:00", 10.2),
            minute_bar("000333.SZ", "SZ", "2026-05-24 14:59:00", 58.8),
        ];
        let frame = minute_bars_to_frame(&bars, "test")?;

        let local_path =
            write_minute_partition_file_local(&staging_dir, "2026-05-24", "SZ", &frame)?;

        let expected = staging_dir.join(minute_partition_key("2026-05-24", "SZ"));
        assert_eq!(local_path, expected);
        assert!(local_path.exists());

        let rows = read_rows(&local_path)?;
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, "000001.SZ");
        assert_eq!(rows[0].1, "SZ");
        assert_eq!(rows[0].2, "2026-05-24");
        assert_eq!(rows[0].3, "2026-05-24 09:31:00");
        assert_eq!(rows[1].3, "2026-05-24 09:32:00");
        assert_eq!(rows[2].0, "000333.SZ");
        assert_eq!(rows[2].2, "2026-05-24");
        assert_eq!(rows[2].3, "2026-05-24 14:59:00");
        assert_eq!(rows[0].4, Some(10.0));
        assert_eq!(rows[1].4, Some(10.2));

        fs::remove_dir_all(staging_dir)?;
        Ok(())
    }

    fn minute_bar(symbol: &str, exchange: &str, time: &str, open: f64) -> MinuteBar1m {
        MinuteBar1m {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            time: time.to_string(),
            open: Some(open),
            high: Some(open + 0.5),
            low: Some(open - 0.5),
            close: Some(open + 0.1),
            volume: Some(1000.0),
            turn_over: Some(10000.0),
            turn_over_rate: Some(0.12),
            factor: 1.0,
            source: Some("test".to_string()),
        }
    }

    #[tokio::test]
    #[ignore = "hits real QMT/TDX and stages parquet locally"]
    async fn real_minute_sync_stages_remote_rows_without_upload() -> Result<()> {
        let config = load_test_qmt_config()?;
        let api = ApiClient::new(
            config.host,
            config.authorization,
            std::time::Duration::from_secs(config.timeout),
        )?;
        let codes = real_minute_codes();
        let start_date = "2026-05-28".to_string();
        let end_date = start_date.clone();

        let grouped =
            fetch_minute_grouped_bars(&api, &codes, &start_date, &end_date, 20, 1).await?;
        assert!(!grouped.is_empty(), "no remote minute rows fetched");

        let staging_dir = temp_dir("real-sync-minute");
        println!("staging_dir={}", staging_dir.display());
        let mut expected_by_key = BTreeMap::new();
        for ((trade_date, exchange), frame) in &grouped {
            let key = minute_partition_key(trade_date, exchange);
            let path =
                write_minute_partition_file_local(&staging_dir, trade_date, exchange, frame)?;
            println!(
                "wrote {} rows={} path={}",
                key,
                frame.height(),
                path.display()
            );
            let bars = minute_bars_from_frame(frame)?;
            expected_by_key.insert(key.clone(), rows_from_minute_bars(&bars));
            let actual = read_full_rows(&path)?;
            assert_eq!(
                actual, expected_by_key[&key],
                "minute parquet mismatch for {key}"
            );
        }

        Ok(())
    }

    fn read_rows(path: &Path) -> Result<Vec<(String, String, String, String, Option<f64>)>> {
        let file = fs::File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let iter = reader.get_row_iter(None)?;
        let mut out = Vec::new();
        for record in iter {
            let record = record?;
            out.push((
                record.get_string(0)?.to_string(),
                record.get_string(1)?.to_string(),
                record.get_string(2)?.to_string(),
                record.get_string(3)?.to_string(),
                record.get_double(4).ok(),
            ));
        }
        Ok(out)
    }

    fn read_full_rows(path: &Path) -> Result<Vec<MinuteRow>> {
        let file = fs::File::open(path)?;
        let reader = SerializedFileReader::new(file)?;
        let iter = reader.get_row_iter(None)?;
        let mut out = Vec::new();
        for record in iter {
            let record = record?;
            out.push((
                record.get_string(0)?.to_string(),
                record.get_string(1)?.to_string(),
                record.get_string(2)?.to_string(),
                record.get_string(3)?.to_string(),
                record.get_double(4).ok(),
                record.get_double(5).ok(),
                record.get_double(6).ok(),
                record.get_double(7).ok(),
                record.get_double(8).ok(),
                record.get_double(9).ok(),
                record.get_double(10).ok(),
                record.get_double(11).ok(),
            ));
        }
        Ok(out)
    }

    type MinuteRow = (
        String,
        String,
        String,
        String,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
    );

    fn rows_from_minute_bars(bars: &[MinuteBar1m]) -> Vec<MinuteRow> {
        bars.iter()
            .map(|bar| {
                (
                    bar.symbol.clone(),
                    bar.exchange.clone(),
                    minute_trade_date(&bar.time).to_string(),
                    bar.time.clone(),
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    bar.turn_over,
                    bar.turn_over_rate,
                    Some(bar.factor),
                )
            })
            .collect()
    }

    fn real_minute_codes() -> Vec<String> {
        vec!["000001.SZ".to_string()]
    }

    #[derive(Debug, Deserialize)]
    struct TestRootConfig {
        qmt: TestQmtConfig,
    }

    #[derive(Debug, Deserialize)]
    struct TestQmtConfig {
        host: String,
        authorization: Option<String>,
        #[serde(default = "default_test_qmt_timeout")]
        timeout: u64,
    }

    fn load_test_qmt_config() -> Result<TestQmtConfig> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config.toml");
        let raw = fs::read_to_string(path)?;
        let config: TestRootConfig = toml::from_str(&raw)?;
        Ok(config.qmt)
    }

    fn default_test_qmt_timeout() -> u64 {
        30
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("temp")
            .join(format!("{prefix}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }
}
