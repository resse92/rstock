use std::collections::BTreeMap;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow_array::builder::{Float64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use clap::Args as ClapArgs;
use csv::StringRecord;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use storage::s3::{build_s3_client, ensure_bucket, S3Client, S3Settings};
use storage::s3::{upload_local_file, validate_parquet_file, write_parquet_bytes_local};
use tokio::task::JoinSet;
use zip::ZipArchive;

use crate::common::{append_manifest_line, load_manifest, parse_opt_f64, source_key};
use crate::config::load_minute_s3_config;

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(long, default_value = "config.toml", help = "配置文件路径")]
    pub config_file: PathBuf,

    #[arg(long, help = "分钟 zip/csv 根目录")]
    pub input_dir: PathBuf,

    #[arg(long, help = "每个 parquet part 最大行数，默认读取 config.toml")]
    pub part_size: Option<usize>,

    #[arg(long, help = "并发上传 parquet part 数，默认读取 config.toml")]
    pub upload_concurrency: Option<usize>,

    #[arg(long, help = "已完成 zip/csv 清单文件，默认读取 config.toml")]
    pub manifest_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct MinuteRow {
    symbol: String,
    exchange: String,
    time: String,
    trade_date: String,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    factor: Option<f64>,
    volume: Option<f64>,
    turnover: Option<f64>,
    turnover_rate: Option<f64>,
    is_paused: Option<f64>,
}

pub async fn run(args: Args) -> Result<()> {
    let file_config = load_minute_s3_config(&args.config_file)?;
    let part_size = args.part_size.unwrap_or(file_config.part_size);
    let upload_concurrency = args
        .upload_concurrency
        .unwrap_or(file_config.upload_concurrency);
    let manifest_file = args
        .manifest_file
        .unwrap_or_else(|| file_config.manifest_file.clone());

    if part_size == 0 {
        return Err(anyhow!("--part-size 必须大于 0"));
    }
    if upload_concurrency == 0 {
        return Err(anyhow!("--upload-concurrency 必须大于 0"));
    }

    let s3_settings = S3Settings {
        endpoint: file_config.host,
        bucket: file_config.bucket,
        access_key: file_config.access_key,
        secret_key: file_config.secret_key,
        region: file_config.region,
    };

    let s3 = build_s3_client(&s3_settings).await?;
    ensure_bucket(&s3, &s3_settings.bucket)
        .await
        .with_context(|| format!("ensure bucket {} failed", s3_settings.bucket))?;

    let files = collect_input_files_recursive(&args.input_dir)?;
    if files.is_empty() {
        return Err(anyhow!(
            "目录中未找到 zip/csv 文件: {}",
            args.input_dir.display()
        ));
    }

    let mut imported = load_manifest(&manifest_file)?;
    let mut next_part_by_partition: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut scanned_rows = 0usize;
    let mut written_rows = 0usize;
    let mut part_files = 0usize;
    let mut skipped = 0usize;

    for (idx, path) in files.iter().enumerate() {
        let manifest_key = source_key(path, &args.input_dir)?;
        if imported.contains(&manifest_key) {
            skipped += 1;
            println!(
                "[SKIP] {}/{} 已完成: {}",
                idx + 1,
                files.len(),
                path.display()
            );
            continue;
        }

        let (source_rows, source_written_rows, source_parts) = process_source(
            path,
            part_size,
            upload_concurrency,
            &s3,
            &s3_settings.bucket,
            &file_config.staging_dir,
            &mut next_part_by_partition,
        )
        .await
        .with_context(|| format!("解析/上传文件失败: {}", path.display()))?;
        scanned_rows += source_rows;
        written_rows += source_written_rows;
        part_files += source_parts;
        append_manifest_line(&manifest_file, &manifest_key)?;
        imported.insert(manifest_key);
        println!(
            "[ZIP] {}/{} 导入完成: {}, 解析 {} 条, 写入 {} 条, {} 个 part",
            idx + 1,
            files.len(),
            path.display(),
            source_rows,
            source_written_rows,
            source_parts
        );
    }

    println!(
        "[DONE] minute_s3 完成: 扫描 {} 条, 写入 {} 条, {} 个 part, 跳过 {} 个文件",
        scanned_rows, written_rows, part_files, skipped
    );
    Ok(())
}

fn collect_input_files_recursive(input_dir: &Path) -> Result<Vec<PathBuf>> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
        for entry in
            std::fs::read_dir(dir).with_context(|| format!("读取目录失败: {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            let ft = entry.file_type()?;
            if ft.is_dir() {
                walk(&path, out)?;
                continue;
            }
            if ft.is_file()
                && path
                    .extension()
                    .and_then(|s| s.to_str())
                    .is_some_and(|ext| {
                        ext.eq_ignore_ascii_case("zip") || ext.eq_ignore_ascii_case("csv")
                    })
            {
                out.push(path);
            }
        }
        Ok(())
    }

    let mut files = Vec::new();
    walk(input_dir, &mut files)?;
    files.sort();
    Ok(files)
}

async fn process_source(
    path: &Path,
    part_size: usize,
    upload_concurrency: usize,
    s3: &S3Client,
    bucket: &str,
    staging_dir: &Path,
    next_part_by_partition: &mut BTreeMap<(String, String), usize>,
) -> Result<(usize, usize, usize)> {
    match path
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase())
        .as_deref()
    {
        Some("zip") => {
            process_zip(
                path,
                part_size,
                upload_concurrency,
                s3,
                bucket,
                staging_dir,
                next_part_by_partition,
            )
            .await
        }
        Some("csv") => {
            let file = std::fs::File::open(path)?;
            process_csv_reader(
                file,
                path,
                part_size,
                upload_concurrency,
                s3,
                bucket,
                staging_dir,
                next_part_by_partition,
            )
            .await
        }
        Some(other) => Err(anyhow!("不支持的文件类型: {other}")),
        None => Err(anyhow!("无法识别文件类型: {}", path.display())),
    }
}

async fn process_zip(
    path: &Path,
    part_size: usize,
    upload_concurrency: usize,
    s3: &S3Client,
    bucket: &str,
    staging_dir: &Path,
    next_part_by_partition: &mut BTreeMap<(String, String), usize>,
) -> Result<(usize, usize, usize)> {
    let file = std::fs::File::open(path)?;
    let mut archive = ZipArchive::new(file)?;
    let mut total_rows = 0usize;
    let mut total_written = 0usize;
    let mut total_parts = 0usize;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        if !entry.is_file()
            || !entry
                .name()
                .rsplit('.')
                .next()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("csv"))
        {
            continue;
        }
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes)?;
        let cursor = Cursor::new(bytes);
        let (rows, written, parts) = process_csv_reader(
            cursor,
            path,
            part_size,
            upload_concurrency,
            s3,
            bucket,
            staging_dir,
            next_part_by_partition,
        )
        .await?;
        total_rows += rows;
        total_written += written;
        total_parts += parts;
    }

    Ok((total_rows, total_written, total_parts))
}

async fn process_csv_reader<R: Read>(
    reader: R,
    source_path: &Path,
    part_size: usize,
    upload_concurrency: usize,
    s3: &S3Client,
    bucket: &str,
    staging_dir: &Path,
    next_part_by_partition: &mut BTreeMap<(String, String), usize>,
) -> Result<(usize, usize, usize)> {
    let mut csv = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(reader);
    let mut buffers: BTreeMap<(String, String), Vec<MinuteRow>> = BTreeMap::new();
    let mut uploads = JoinSet::new();
    let mut scanned = 0usize;
    let mut written = 0usize;
    let mut parts = 0usize;

    for record in csv.records() {
        let row = match parse_minute_row(&record?) {
            Some(row) => row,
            None => continue,
        };
        scanned += 1;
        push_row(
            row,
            part_size,
            upload_concurrency,
            s3,
            bucket,
            staging_dir,
            next_part_by_partition,
            &mut buffers,
            &mut uploads,
            &mut written,
            &mut parts,
        )
        .await
        .with_context(|| format!("处理记录失败: {}", source_path.display()))?;
    }

    flush_all_buffers(
        s3,
        bucket,
        staging_dir,
        next_part_by_partition,
        &mut buffers,
        &mut uploads,
        &mut written,
        &mut parts,
    )
    .await?;
    drain_uploads(&mut uploads).await?;
    Ok((scanned, written, parts))
}

async fn push_row(
    row: MinuteRow,
    part_size: usize,
    upload_concurrency: usize,
    s3: &S3Client,
    bucket: &str,
    staging_dir: &Path,
    next_part_by_partition: &mut BTreeMap<(String, String), usize>,
    buffers: &mut BTreeMap<(String, String), Vec<MinuteRow>>,
    uploads: &mut JoinSet<Result<()>>,
    written: &mut usize,
    parts: &mut usize,
) -> Result<()> {
    let key = (row.trade_date.clone(), row.exchange.clone());
    let buf = buffers.entry(key.clone()).or_default();
    buf.push(row);
    if buf.len() < part_size {
        return Ok(());
    }
    let rows = std::mem::take(buf);
    flush_rows(
        key,
        rows,
        s3,
        bucket,
        staging_dir,
        next_part_by_partition,
        uploads,
        written,
        parts,
    )
    .await?;
    wait_one_upload(upload_concurrency, uploads).await
}

async fn flush_all_buffers(
    s3: &S3Client,
    bucket: &str,
    staging_dir: &Path,
    next_part_by_partition: &mut BTreeMap<(String, String), usize>,
    buffers: &mut BTreeMap<(String, String), Vec<MinuteRow>>,
    uploads: &mut JoinSet<Result<()>>,
    written: &mut usize,
    parts: &mut usize,
) -> Result<()> {
    let keys: Vec<(String, String)> = buffers.keys().cloned().collect();
    for key in keys {
        let Some(rows) = buffers.remove(&key) else {
            continue;
        };
        if rows.is_empty() {
            continue;
        }
        flush_rows(
            key,
            rows,
            s3,
            bucket,
            staging_dir,
            next_part_by_partition,
            uploads,
            written,
            parts,
        )
        .await?;
    }
    Ok(())
}

async fn flush_rows(
    key: (String, String),
    rows: Vec<MinuteRow>,
    s3: &S3Client,
    bucket: &str,
    staging_dir: &Path,
    next_part_by_partition: &mut BTreeMap<(String, String), usize>,
    uploads: &mut JoinSet<Result<()>>,
    written: &mut usize,
    parts: &mut usize,
) -> Result<()> {
    let (trade_date, exchange) = key;
    let mut deduped = dedup_rows(rows);
    deduped.sort_by(|a, b| a.symbol.cmp(&b.symbol).then(a.time.cmp(&b.time)));
    if deduped.is_empty() {
        return Ok(());
    }
    let part_no = next_part_by_partition
        .entry((trade_date.clone(), exchange.clone()))
        .or_default();
    let key = format!(
        "curated/minute_bars_1m/trade_date={trade_date}/exchange={exchange}/part-{part_no:06}.parquet"
    );
    *part_no += 1;
    let local_path = staging_dir.join(&key);
    let parquet_bytes = to_parquet_bytes(&deduped)?;
    write_parquet_bytes_local(&local_path, parquet_bytes)?;
    validate_parquet_file(&local_path)?;
    *written += deduped.len();
    *parts += 1;
    let bucket = bucket.to_string();
    let key_clone = key.clone();
    let local_path_clone = local_path.clone();
    let s3_owned = s3.clone();
    uploads.spawn(async move {
        upload_minute_part_owned(s3_owned, bucket, key_clone, local_path_clone).await
    });
    Ok(())
}

async fn wait_one_upload(limit: usize, uploads: &mut JoinSet<Result<()>>) -> Result<()> {
    if uploads.len() < limit {
        return Ok(());
    }
    if let Some(result) = uploads.join_next().await {
        result??;
    }
    Ok(())
}

async fn drain_uploads(uploads: &mut JoinSet<Result<()>>) -> Result<()> {
    while let Some(result) = uploads.join_next().await {
        result??;
    }
    Ok(())
}

async fn upload_minute_part_owned(
    s3: S3Client,
    bucket: String,
    key: String,
    local_path: PathBuf,
) -> Result<()> {
    upload_local_file(&s3, &bucket, &key, &local_path).await?;
    println!("[UPLOAD] s3://{bucket}/{key}");
    Ok(())
}

fn parse_minute_row(row: &StringRecord) -> Option<MinuteRow> {
    let symbol = row.get(0)?.trim();
    let time = row.get(1)?.trim();
    let exchange = symbol.split('.').nth(1)?.trim();
    if symbol.is_empty() || exchange.is_empty() || time.len() < 19 {
        return None;
    }
    Some(MinuteRow {
        symbol: symbol.to_string(),
        exchange: exchange.to_string(),
        time: time[0..19].to_string(),
        trade_date: time[0..10].to_string(),
        open: parse_opt_f64(row.get(2)),
        high: parse_opt_f64(row.get(3)),
        low: parse_opt_f64(row.get(4)),
        close: parse_opt_f64(row.get(5)),
        factor: parse_opt_f64(row.get(6)),
        volume: parse_opt_f64(row.get(7)),
        turnover: parse_opt_f64(row.get(8)),
        turnover_rate: parse_opt_f64(row.get(9)),
        is_paused: parse_opt_f64(row.get(10)),
    })
}

fn dedup_rows(rows: Vec<MinuteRow>) -> Vec<MinuteRow> {
    let mut keyed = BTreeMap::new();
    for row in rows {
        keyed.insert((row.symbol.clone(), row.time.clone()), row);
    }
    keyed.into_values().collect()
}

fn to_parquet_bytes(rows: &[MinuteRow]) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("time", DataType::Utf8, false),
        Field::new("trade_date", DataType::Utf8, false),
        Field::new("open", DataType::Float64, true),
        Field::new("high", DataType::Float64, true),
        Field::new("low", DataType::Float64, true),
        Field::new("close", DataType::Float64, true),
        Field::new("factor", DataType::Float64, true),
        Field::new("volume", DataType::Float64, true),
        Field::new("turnover", DataType::Float64, true),
        Field::new("turnover_rate", DataType::Float64, true),
        Field::new("is_paused", DataType::Float64, true),
    ]));

    let mut symbol = StringBuilder::new();
    let mut exchange = StringBuilder::new();
    let mut time = StringBuilder::new();
    let mut trade_date = StringBuilder::new();
    let mut open = Float64Builder::new();
    let mut high = Float64Builder::new();
    let mut low = Float64Builder::new();
    let mut close = Float64Builder::new();
    let mut factor = Float64Builder::new();
    let mut volume = Float64Builder::new();
    let mut turnover = Float64Builder::new();
    let mut turnover_rate = Float64Builder::new();
    let mut is_paused = Float64Builder::new();

    for row in rows {
        symbol.append_value(&row.symbol);
        exchange.append_value(&row.exchange);
        time.append_value(&row.time);
        trade_date.append_value(&row.trade_date);
        open.append_option(row.open);
        high.append_option(row.high);
        low.append_option(row.low);
        close.append_option(row.close);
        factor.append_option(row.factor);
        volume.append_option(row.volume);
        turnover.append_option(row.turnover);
        turnover_rate.append_option(row.turnover_rate);
        is_paused.append_option(row.is_paused);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(symbol.finish()) as ArrayRef,
            Arc::new(exchange.finish()) as ArrayRef,
            Arc::new(time.finish()) as ArrayRef,
            Arc::new(trade_date.finish()) as ArrayRef,
            Arc::new(open.finish()) as ArrayRef,
            Arc::new(high.finish()) as ArrayRef,
            Arc::new(low.finish()) as ArrayRef,
            Arc::new(close.finish()) as ArrayRef,
            Arc::new(factor.finish()) as ArrayRef,
            Arc::new(volume.finish()) as ArrayRef,
            Arc::new(turnover.finish()) as ArrayRef,
            Arc::new(turnover_rate.finish()) as ArrayRef,
            Arc::new(is_paused.finish()) as ArrayRef,
        ],
    )?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(128 * 1024)
        .build();
    let mut cursor = Cursor::new(Vec::new());
    {
        let mut writer = ArrowWriter::try_new(&mut cursor, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
    }
    Ok(cursor.into_inner())
}
