use std::collections::BTreeMap;
use std::io::Cursor;
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
use zip::ZipArchive;

use crate::common::{
    append_manifest_line, collect_files, load_manifest, open_file, output_id, parse_opt_f64,
    source_key, write_file,
};

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(long, help = "分钟 zip 根目录")]
    pub input_dir: PathBuf,
    #[arg(long, help = "Parquet 输出根目录")]
    pub output_dir: PathBuf,
    #[arg(long, default_value_t = 200_000, help = "每个 parquet part 最大行数")]
    pub part_size: usize,
    #[arg(
        long,
        default_value = "meta/ingestion/minute_zip_to_parquet_manifest.txt",
        help = "已转换 zip 清单文件"
    )]
    pub manifest_file: PathBuf,
}

#[derive(Debug, Clone)]
struct Row {
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

pub fn run(args: Args) -> Result<()> {
    if args.part_size == 0 {
        return Err(anyhow!("--part-size 必须大于 0"));
    }
    let files = collect_files(&args.input_dir, &["zip"], true)?;
    if files.is_empty() {
        return Err(anyhow!(
            "目录中未找到 zip 文件: {}",
            args.input_dir.display()
        ));
    }
    let mut converted = load_manifest(&args.manifest_file)?;
    let mut scanned = 0usize;
    let mut written = 0usize;
    let mut parts = 0usize;
    let mut skipped = 0usize;
    for (idx, path) in files.iter().enumerate() {
        let key = source_key(path, &args.input_dir)?;
        if converted.contains(&key) {
            skipped += 1;
            println!(
                "[SKIP] {}/{} 已完成: {}",
                idx + 1,
                files.len(),
                path.display()
            );
            continue;
        }
        let (groups, zip_rows) =
            parse_zip_groups(path).with_context(|| format!("解析 zip 失败: {}", path.display()))?;
        scanned += zip_rows;
        let file_id = output_id(path, &args.input_dir, idx);
        let mut file_written = 0usize;
        let mut file_parts = 0usize;
        for ((trade_date, exchange), rows) in groups {
            let mut deduped = dedup_rows(rows);
            deduped.sort_by(|a, b| a.symbol.cmp(&b.symbol).then(a.time.cmp(&b.time)));
            for (part_idx, chunk) in deduped.chunks(args.part_size).enumerate() {
                let out = args
                    .output_dir
                    .join("curated/minute_bars_1m")
                    .join(format!("trade_date={trade_date}"))
                    .join(format!("exchange={exchange}"))
                    .join(format!("{file_id}-part-{part_idx:03}.parquet"));
                write_file(&out, to_parquet_bytes(chunk)?)?;
                file_written += chunk.len();
                file_parts += 1;
            }
        }
        append_manifest_line(&args.manifest_file, &key)?;
        converted.insert(key);
        written += file_written;
        parts += file_parts;
        println!(
            "[ZIP] {}/{} 转换完成: {}, 解析 {} 条, 写入 {} 条, {} 个 part",
            idx + 1,
            files.len(),
            path.display(),
            zip_rows,
            file_written,
            file_parts
        );
    }
    println!(
        "[DONE] minute 完成: 扫描 {} 条, 写入 {} 条, {} 个 part, 跳过 {} 个 zip",
        scanned, written, parts, skipped
    );
    Ok(())
}

fn parse_zip_groups(path: &Path) -> Result<(BTreeMap<(String, String), Vec<Row>>, usize)> {
    let mut archive = ZipArchive::new(open_file(path)?)?;
    let mut groups: BTreeMap<(String, String), Vec<Row>> = BTreeMap::new();
    let mut count = 0usize;
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
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(&mut entry);
        for record in reader.records() {
            if let Some(row) = parse_row(&record?) {
                groups
                    .entry((row.trade_date.clone(), row.exchange.clone()))
                    .or_default()
                    .push(row);
                count += 1;
            }
        }
    }
    Ok((groups, count))
}

fn parse_row(row: &StringRecord) -> Option<Row> {
    let symbol = row.get(0)?.trim();
    let time = row.get(1)?.trim();
    let exchange = symbol.split('.').nth(1)?.trim();
    if exchange.is_empty() || time.len() < 19 {
        return None;
    }
    Some(Row {
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

fn dedup_rows(rows: Vec<Row>) -> Vec<Row> {
    let mut keyed = BTreeMap::new();
    for row in rows {
        keyed.insert((row.symbol.clone(), row.time.clone()), row);
    }
    keyed.into_values().collect()
}

fn to_parquet_bytes(rows: &[Row]) -> Result<Vec<u8>> {
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
    let mut writer = ArrowWriter::try_new(&mut cursor, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(cursor.into_inner())
}
