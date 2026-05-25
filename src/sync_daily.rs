use std::cmp::{max, min};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{Datelike, NaiveDate};
use clap::Args;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use crate::api::ApiClient;
use crate::models::{DailyBar, MarketRequest, DEFAULT_TIMEOUT_SECS};
use crate::normalize::normalize_full_kline_response;
use crate::s3::{build_s3_client, ensure_bucket, upload_daily_partition_file_staged, S3Settings};
use crate::tdx_source;
use crate::utils::{chunked, load_stock_codes_from_file};

#[derive(Debug, Args, Clone)]
pub struct SyncDailyArgs {
    #[arg(long, help = "开始日期，YYYY-MM-DD 或 YYYYMMDD")]
    pub start_date: String,

    #[arg(long, help = "结束日期，YYYY-MM-DD 或 YYYYMMDD")]
    pub end_date: String,

    #[arg(long, default_value_t = 200)]
    pub chunk_size: usize,

    #[arg(long, default_value_t = 8)]
    pub fetch_concurrency: usize,

    #[arg(long, default_value_t = false)]
    pub incremental: bool,

    #[arg(long, default_value = "meta/ingestion/daily_watermark.txt")]
    pub watermark_file: PathBuf,

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

pub async fn run_sync_daily(args: SyncDailyArgs) -> Result<()> {
    if args.chunk_size == 0 {
        return Err(anyhow!("--chunk-size 必须大于 0"));
    }
    if args.fetch_concurrency == 0 {
        return Err(anyhow!("--fetch-concurrency 必须大于 0"));
    }

    let mut start_date_compact = compact_date(&args.start_date)?;
    let end_date_compact = compact_date(&args.end_date)?;
    let mut start_day = parse_compact_date(&start_date_compact)?;
    let end_day = parse_compact_date(&end_date_compact)?;

    if args.incremental {
        if let Some(watermark_day) = read_watermark_day(&args.watermark_file)? {
            if let Some(next_day) = watermark_day.succ_opt() {
                if next_day > start_day {
                    start_day = next_day;
                    start_date_compact = format_compact_date(start_day);
                }
            }
            println!(
                "[INFO] incremental 模式: watermark={}, 实际开始日期={}",
                format_compact_date(watermark_day),
                start_date_compact
            );
        } else {
            println!(
                "[INFO] incremental 模式: 未找到 watermark 文件 {}, 按参数全量开始",
                args.watermark_file.display()
            );
        }
    }

    if start_day > end_day {
            println!(
                "[DONE] 无需同步: 开始日期 {} 晚于结束日期 {}",
                start_date_compact, end_date_compact
            );
        return Ok(());
    }

    let s3_host = args
        .s3_host
        .or_else(|| env_var_any(&["S3_HOST", "s3_host"]))
        .ok_or_else(|| anyhow!("缺少 S3 host，请在 .env 设置 s3_host 或 S3_HOST"))?;

    let api = ApiClient::new(
        args.base_url,
        args.authorization,
        Duration::from_secs(args.timeout),
    )?;

    let stock_codes = load_sync_stock_codes(&api, args.stock_codes_file.as_ref()).await?;
    println!("[INFO] 使用股票 {} 只", stock_codes.len());

    let s3_settings = S3Settings {
        endpoint: s3_host,
        bucket: args.s3_bucket,
        access_key: args.s3_access_key,
        secret_key: args.s3_secret_key,
        region: args.s3_region,
    };

    let s3 = build_s3_client(&s3_settings).await?;
    ensure_bucket(&s3, &s3_settings.bucket)
        .await
        .with_context(|| format!("ensure bucket {} failed", s3_settings.bucket))?;
    let bucket_name = s3_settings.bucket.clone();
    let staging_dir = args.staging_dir.clone();
    let windows = month_windows(&start_date_compact, &end_date_compact)?;
    let total_months = windows.len();
    let batches = chunked(&stock_codes, args.chunk_size);
    let total_batches = batches.len();

    println!(
        "[INFO] 开始拉取日线: {} 只股票, {} 批/月, {} 个月, 区间 {} ~ {}",
        stock_codes.len(),
        total_batches,
        total_months,
        start_date_compact,
        end_date_compact
    );

    #[derive(Debug)]
    enum WriteJob {
        Batch {
            bars: Vec<DailyBar>,
        },
        FlushMonth {
            month_idx: usize,
            total_months: usize,
            ack: oneshot::Sender<Result<()>>,
        },
    }

    let writer_bucket = bucket_name.clone();
    let (tx, mut rx) = mpsc::channel::<WriteJob>(8);
    let writer = tokio::spawn(async move {
        let mut uploaded_files: BTreeSet<String> = BTreeSet::new();
        let mut written_rows = 0usize;
        let mut month_partition_cache: BTreeMap<(String, String, String), Vec<DailyBar>> =
            BTreeMap::new();

        while let Some(job) = rx.recv().await {
            match job {
                WriteJob::Batch { bars } => {
                    for bar in bars {
                        if bar.time.len() < 7 {
                            continue;
                        }
                        let year = bar.time[0..4].to_string();
                        let month = bar.time[5..7].to_string();
                        let key = (bar.exchange.clone(), year, month);
                        month_partition_cache.entry(key).or_default().push(bar);
                        written_rows += 1;
                    }
                }
                WriteJob::FlushMonth {
                    month_idx,
                    total_months,
                    ack,
                } => {
                    let upload_result = async {
                        let mut uploaded_now = 0usize;
                        for ((exchange, year, month), rows) in month_partition_cache {
                            let deduped_rows = dedup_daily_rows(rows);
                            let key = upload_daily_partition_file_staged(
                                &s3,
                                &writer_bucket,
                                &exchange,
                                &year,
                                &month,
                                &deduped_rows,
                                &staging_dir,
                            )
                            .await?;
                            uploaded_files.insert(key);
                            uploaded_now += 1;
                        }

                        println!(
                            "[WRITER] 月 {}/{} 上传完成: {} 个分区文件, 累计 {} 条/{} 文件",
                            month_idx,
                            total_months,
                            uploaded_now,
                            written_rows,
                            uploaded_files.len()
                        );
                        Ok::<(), anyhow::Error>(())
                    }
                    .await;

                    month_partition_cache = BTreeMap::new();
                    if let Err(err) = upload_result {
                        let _ = ack.send(Err(anyhow!("{err:#}")));
                        return Err(err);
                    }
                    let _ = ack.send(Ok(()));
                }
            }
        }

        Ok::<(usize, usize), anyhow::Error>((written_rows, uploaded_files.len()))
    });

    for (month_idx, (month_start, month_end)) in windows.into_iter().enumerate() {
        println!(
            "[MONTH] {}/{} 拉取 {} ~ {}",
            month_idx + 1,
            total_months,
            month_start,
            month_end
        );

        let mut join_set = JoinSet::new();
        let mut next_batch_idx = 0usize;
        while next_batch_idx < total_batches || !join_set.is_empty() {
            while next_batch_idx < total_batches && join_set.len() < args.fetch_concurrency {
                let batch_no = next_batch_idx + 1;
                let chunk = batches[next_batch_idx].clone();
                let month_start_clone = month_start.clone();
                let month_end_clone = month_end.clone();
                let month_start_api = dashed_date(&month_start_clone)?;
                let month_end_api = dashed_date(&month_end_clone)?;
                let api_clone = api.clone();
                join_set.spawn(async move {
                    let req = MarketRequest::new(
                        chunk.clone(),
                        "1d",
                        month_start_api.clone(),
                        month_end_api.clone(),
                        "none",
                    );
                    let rows = match api_clone.fetch_market_batch(&req).await {
                        Ok(resp) => normalize_full_kline_response(&resp, "1d"),
                        Err(err) => {
                            eprintln!("[QMT][daily] 批次 {batch_no} 失败，切换 TDX 兜底: {err:#}");
                            Vec::new()
                        }
                    };
                    let mut batch_bars: Vec<DailyBar> = Vec::new();
                    for row in &rows {
                        if let Some(bar) = DailyBar::from_normalized(row) {
                            batch_bars.push(bar);
                        }
                    }
                    let found: BTreeSet<String> =
                        batch_bars.iter().map(|bar| bar.symbol.clone()).collect();
                    let missing = chunk
                        .iter()
                        .filter(|code| !found.contains(*code))
                        .cloned()
                        .collect::<Vec<_>>();
                    if !missing.is_empty() {
                        eprintln!(
                            "[QMT][daily] 批次 {batch_no} 缺少 {} 只股票，切换 TDX 兜底",
                            missing.len()
                        );
                        batch_bars.extend(tdx_source::fetch_daily_bars(
                            &missing,
                            &month_start_clone,
                            &month_end_clone,
                        )?);
                    }
                    Ok::<(usize, usize, Vec<DailyBar>), anyhow::Error>((
                        batch_no,
                        batch_bars.len(),
                        batch_bars,
                    ))
                });
                next_batch_idx += 1;
            }

            let finished = join_set
                .join_next()
                .await
                .ok_or_else(|| anyhow!("fetch 并发任务异常结束"))?;
            let (batch_no, row_count, bars) =
                finished.map_err(|e| anyhow!("fetch task join error: {e}"))??;

            tx.send(WriteJob::Batch { bars })
                .await
                .map_err(|_| anyhow!("写入队列已关闭"))?;

            println!(
                "[FETCHER] 月 {}/{} 批次 {}/{}: 解析 {} 条",
                month_idx + 1,
                total_months,
                batch_no,
                total_batches,
                row_count
            );
        }

        let (ack_tx, ack_rx) = oneshot::channel();
        tx.send(WriteJob::FlushMonth {
            month_idx: month_idx + 1,
            total_months,
            ack: ack_tx,
        })
        .await
        .map_err(|_| anyhow!("写入队列已关闭"))?;

        ack_rx
            .await
            .map_err(|_| anyhow!("writer flush ack 通道已关闭"))??;

        if args.incremental {
            write_watermark_day(&args.watermark_file, &month_end)?;
            println!(
                "[INFO] 更新 watermark: {} -> {}",
                args.watermark_file.display(),
                month_end
            );
        }
    }

    drop(tx);
    let (written_rows, uploaded_files) = writer
        .await
        .map_err(|e| anyhow!("writer task join error: {e}"))??;

    if args.incremental {
        write_watermark_day(&args.watermark_file, &end_date_compact)?;
        println!(
            "[INFO] 最终 watermark 已更新: {} -> {}",
            args.watermark_file.display(),
            end_date_compact
        );
    }

    println!(
        "[DONE] 上传完成: 累计写入 {} 条日线, {} 个分区文件, bucket={}",
        written_rows, uploaded_files, bucket_name
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

#[cfg_attr(not(test), allow(dead_code))]
async fn fetch_daily_bars_with_fallback(
    api: &ApiClient,
    stock_codes: &[String],
    start_date: &str,
    end_date: &str,
    chunk_size: usize,
    fetch_concurrency: usize,
) -> Result<Vec<DailyBar>> {
    let start_date = compact_date(start_date)?;
    let end_date = compact_date(end_date)?;
    let windows = month_windows(&start_date, &end_date)?;
    let batches = chunked(stock_codes, chunk_size);
    let total_batches = batches.len();
    let total_months = windows.len();
    let mut all_bars = Vec::new();

    for (month_idx, (month_start, month_end)) in windows.into_iter().enumerate() {
        let mut join_set = JoinSet::new();
        let mut next_batch_idx = 0usize;

        while next_batch_idx < total_batches || !join_set.is_empty() {
            while next_batch_idx < total_batches && join_set.len() < fetch_concurrency {
                let batch_no = next_batch_idx + 1;
                let chunk = batches[next_batch_idx].clone();
                let month_start_clone = month_start.clone();
                let month_end_clone = month_end.clone();
                let month_start_api = dashed_date(&month_start_clone)?;
                let month_end_api = dashed_date(&month_end_clone)?;
                let api_clone = api.clone();
                join_set.spawn(async move {
                    let req = MarketRequest::new(
                        chunk.clone(),
                        "1d",
                        month_start_api.clone(),
                        month_end_api.clone(),
                        "none",
                    );
                    let rows = match api_clone.fetch_market_batch(&req).await {
                        Ok(resp) => normalize_full_kline_response(&resp, "1d"),
                        Err(err) => {
                            eprintln!("[QMT][daily] 批次 {batch_no} 失败，切换 TDX 兜底: {err:#}");
                            Vec::new()
                        }
                    };
                    let mut batch_bars: Vec<DailyBar> = Vec::new();
                    for row in &rows {
                        if let Some(bar) = DailyBar::from_normalized(row) {
                            batch_bars.push(bar);
                        }
                    }
                    let found: BTreeSet<String> =
                        batch_bars.iter().map(|bar| bar.symbol.clone()).collect();
                    let missing = chunk
                        .iter()
                        .filter(|code| !found.contains(*code))
                        .cloned()
                        .collect::<Vec<_>>();
                    if !missing.is_empty() {
                        eprintln!(
                            "[QMT][daily] 批次 {batch_no} 缺少 {} 只股票，切换 TDX 兜底",
                            missing.len()
                        );
                        batch_bars.extend(tdx_source::fetch_daily_bars(
                            &missing,
                            &month_start_clone,
                            &month_end_clone,
                        )?);
                    }
                    Ok::<(usize, Vec<DailyBar>), anyhow::Error>((batch_no, batch_bars))
                });
                next_batch_idx += 1;
            }

            let finished = join_set
                .join_next()
                .await
                .ok_or_else(|| anyhow!("fetch 并发任务异常结束"))?;
            let (batch_no, bars) =
                finished.map_err(|e| anyhow!("fetch task join error: {e}"))??;
            println!(
                "[FETCHER] 月 {}/{} 批次 {}/{}: 解析 {} 条",
                month_idx + 1,
                total_months,
                batch_no,
                total_batches,
                bars.len()
            );
            all_bars.extend(bars);
        }
    }

    Ok(all_bars)
}

#[cfg_attr(not(test), allow(dead_code))]
fn stage_daily_bars_local(
    staging_dir: &std::path::Path,
    bars: Vec<DailyBar>,
) -> Result<Vec<(String, PathBuf, usize)>> {
    let mut groups: BTreeMap<(String, String, String), Vec<DailyBar>> = BTreeMap::new();
    for bar in bars {
        if bar.time.len() < 7 {
            continue;
        }
        let year = bar.time[0..4].to_string();
        let month = bar.time[5..7].to_string();
        groups
            .entry((bar.exchange.clone(), year, month))
            .or_default()
            .push(bar);
    }

    let mut out = Vec::new();
    for ((exchange, year, month), rows) in groups {
        let deduped = dedup_daily_rows(rows);
        let key =
            format!("curated/daily_bars/exchange={exchange}/year={year}/month={month}/data.parquet");
        let path = staging_dir.join(&key);
        crate::s3::write_daily_partition_file_local(&path, &deduped)?;
        crate::s3::validate_parquet_file(&path)?;
        out.push((key, path, deduped.len()));
    }
    Ok(out)
}

fn dedup_daily_rows(rows: Vec<DailyBar>) -> Vec<DailyBar> {
    let mut keyed: BTreeMap<(String, String), DailyBar> = BTreeMap::new();
    for row in rows {
        keyed.insert((row.symbol.clone(), row.time.clone()), row);
    }
    let mut out: Vec<DailyBar> = keyed.into_values().collect();
    out.sort_by(|a, b| {
        a.symbol
            .cmp(&b.symbol)
            .then(a.time.cmp(&b.time))
            .then(a.exchange.cmp(&b.exchange))
    });
    out
}

fn read_watermark_day(path: &PathBuf) -> Result<Option<NaiveDate>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(path)
        .with_context(|| format!("读取 watermark 失败: {}", path.display()))?;
    let val = raw.trim();
    if val.is_empty() {
        return Ok(None);
    }
    let day = compact_date(val)?;
    Ok(Some(parse_compact_date(&day)?))
}

fn write_watermark_day(path: &PathBuf, day_compact: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("创建 watermark 目录失败: {}", parent.display()))?;
    }
    fs::write(path, day_compact.as_bytes())
        .with_context(|| format!("写入 watermark 失败: {}", path.display()))?;
    Ok(())
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

fn dashed_date(input: &str) -> Result<String> {
    let compact = compact_date(input)?;
    Ok(format!(
        "{}-{}-{}",
        &compact[0..4],
        &compact[4..6],
        &compact[6..8]
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

fn parse_compact_date(input: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(input, "%Y%m%d")
        .with_context(|| format!("无效日期: {input}，应为 YYYYMMDD"))
}

fn format_compact_date(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}

fn month_windows(start_compact: &str, end_compact: &str) -> Result<Vec<(String, String)>> {
    let start = parse_compact_date(start_compact)?;
    let end = parse_compact_date(end_compact)?;
    if start > end {
        return Err(anyhow!(
            "开始日期不能晚于结束日期: {start_compact} > {end_compact}"
        ));
    }

    let mut out = Vec::new();
    let mut cursor = NaiveDate::from_ymd_opt(start.year(), start.month(), 1)
        .ok_or_else(|| anyhow!("无效月份: {}-{}", start.year(), start.month()))?;

    while cursor <= end {
        let month_start = max(cursor, start);
        let next_month = if cursor.month() == 12 {
            NaiveDate::from_ymd_opt(cursor.year() + 1, 1, 1)
        } else {
            NaiveDate::from_ymd_opt(cursor.year(), cursor.month() + 1, 1)
        }
        .ok_or_else(|| anyhow!("无效下个月日期"))?;
        let month_end = min(
            next_month
                .pred_opt()
                .ok_or_else(|| anyhow!("无效月末日期"))?,
            end,
        );

        out.push((
            format_compact_date(month_start),
            format_compact_date(month_end),
        ));
        cursor = next_month;
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{dedup_daily_rows, fetch_daily_bars_with_fallback, stage_daily_bars_local};
    use crate::api::ApiClient;
    use crate::models::{DailyBar, DEFAULT_TIMEOUT_SECS};
    use crate::s3::write_daily_partition_file_local;
    use anyhow::Result;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use parquet::record::RowAccessor;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn dedup_daily_rows_keeps_latest_row_per_symbol_and_day() {
        let rows = vec![
            daily_bar("000001.SZ", "SZ", "2026-05-20", 10.0),
            daily_bar("000001.SZ", "SZ", "2026-05-20", 11.0),
            daily_bar("000001.SZ", "SZ", "2026-05-21", 12.0),
            daily_bar("600000.SH", "SH", "2026-05-20", 8.0),
        ];

        let deduped = dedup_daily_rows(rows);

        assert_eq!(deduped.len(), 3);
        assert_eq!(deduped[0].symbol, "000001.SZ");
        assert_eq!(deduped[0].time, "2026-05-20");
        assert_eq!(deduped[0].open, Some(11.0));
        assert_eq!(deduped[1].time, "2026-05-21");
        assert_eq!(deduped[2].symbol, "600000.SH");
    }

    #[test]
    fn writes_expected_daily_partition_file_to_local_staging() -> Result<()> {
        let staging_dir = temp_dir("sync-daily");
        let relative = "curated/daily_bars/exchange=SZ/year=2026/month=05/data.parquet";
        let local_path = staging_dir.join(relative);
        let rows = dedup_daily_rows(vec![
            daily_bar("000001.SZ", "SZ", "2026-05-20", 10.0),
            daily_bar("000001.SZ", "SZ", "2026-05-20", 11.0),
            daily_bar("000002.SZ", "SZ", "2026-05-21", 20.0),
        ]);

        let written = write_daily_partition_file_local(&local_path, &rows)?;
        assert_eq!(written, local_path);
        assert!(written.exists());

        let parquet_rows = read_rows(&written)?;
        assert_eq!(parquet_rows.len(), 2);
        assert_eq!(parquet_rows[0].0, "000001.SZ");
        assert_eq!(parquet_rows[0].1, "SZ");
        assert_eq!(parquet_rows[0].2, "2026-05-20");
        assert_eq!(parquet_rows[0].3, Some(11.0));
        assert_eq!(parquet_rows[1].0, "000002.SZ");
        assert_eq!(parquet_rows[1].2, "2026-05-21");

        fs::remove_dir_all(staging_dir)?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "hits real QMT/TDX and stages parquet locally"]
    async fn real_daily_sync_stages_remote_rows_without_upload() -> Result<()> {
        dotenvy::dotenv().ok();
        let api = ApiClient::new(
            std::env::var("QMT_API_HOST").unwrap_or_else(|_| "http://127.0.0.1:8000".to_string()),
            std::env::var("QMT_API_AUTHORIZATION").ok(),
            std::time::Duration::from_secs(DEFAULT_TIMEOUT_SECS),
        )?;
        let codes = real_daily_codes(&api).await?;
        let start_date = "2025-01-02".to_string();
        let end_date = start_date.clone();

        let fetched = fetch_daily_bars_with_fallback(&api, &codes, &start_date, &end_date, 20, 1).await?;
        assert!(!fetched.is_empty(), "no remote daily rows fetched");

        let mut expected_groups: BTreeMap<String, Vec<DailyRow>> = BTreeMap::new();
        let mut grouped: BTreeMap<(String, String, String), Vec<DailyBar>> = BTreeMap::new();
        for bar in fetched {
            let year = bar.time[0..4].to_string();
            let month = bar.time[5..7].to_string();
            grouped
                .entry((bar.exchange.clone(), year, month))
                .or_default()
                .push(bar);
        }
        for ((exchange, year, month), rows) in grouped {
            let key =
                format!("curated/daily_bars/exchange={exchange}/year={year}/month={month}/data.parquet");
            let deduped = dedup_daily_rows(rows);
            expected_groups.insert(key, rows_from_daily_bars(&deduped));
        }

        let staging_dir = temp_dir("real-sync-daily");
        let staged = stage_daily_bars_local(&staging_dir, expected_groups.values().flatten().cloned().map(daily_row_to_bar).collect())?;
        for (key, path, _) in staged {
            let actual = read_full_rows(&path)?;
            assert_eq!(actual, expected_groups[&key], "daily parquet mismatch for {key}");
        }

        // fs::remove_dir_all(staging_dir)?;
        Ok(())
    }

    fn daily_bar(symbol: &str, exchange: &str, time: &str, open: f64) -> DailyBar {
        DailyBar {
            symbol: symbol.to_string(),
            exchange: exchange.to_string(),
            time: time.to_string(),
            open: Some(open),
            high: Some(open + 1.0),
            low: Some(open - 1.0),
            close: Some(open + 0.5),
            volume: Some(1000.0),
            amount: Some(10000.0),
            adj_factor: Some(1.0),
            settle: None,
            open_interest: None,
            source: Some("test".to_string()),
        }
    }

    fn read_rows(path: &Path) -> Result<Vec<(String, String, String, Option<f64>)>> {
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
                record.get_double(3).ok(),
            ));
        }
        Ok(out)
    }

    fn read_full_rows(path: &Path) -> Result<Vec<DailyRow>> {
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
                record.get_double(3).ok(),
                record.get_double(4).ok(),
                record.get_double(5).ok(),
                record.get_double(6).ok(),
                record.get_double(7).ok(),
                record.get_double(8).ok(),
            ));
        }
        Ok(out)
    }

    type DailyRow = (
        String,
        String,
        String,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
        Option<f64>,
    );

    fn rows_from_daily_bars(bars: &[DailyBar]) -> Vec<DailyRow> {
        bars.iter()
            .map(|bar| {
                (
                    bar.symbol.clone(),
                    bar.exchange.clone(),
                    bar.time.clone(),
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    bar.amount,
                )
            })
            .collect()
    }

    fn daily_row_to_bar(row: DailyRow) -> DailyBar {
        DailyBar {
            symbol: row.0,
            exchange: row.1,
            time: row.2,
            open: row.3,
            high: row.4,
            low: row.5,
            close: row.6,
            volume: row.7,
            amount: row.8,
            adj_factor: None,
            settle: None,
            open_interest: None,
            source: Some("test".to_string()),
        }
    }

    async fn real_daily_codes(api: &ApiClient) -> Result<Vec<String>> {
        api.discover_all_stock_codes().await
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("temp")
            .join(format!("{prefix}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }
}
