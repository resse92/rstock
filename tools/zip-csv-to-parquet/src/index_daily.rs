use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow_array::builder::{Float64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use clap::Args as ClapArgs;
use csv::StringRecord;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::common::{collect_files, open_file, parse_opt_f64, write_file};

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(long, help = "指数日线 csv 目录")]
    pub input_dir: PathBuf,
    #[arg(long, help = "Parquet 输出根目录")]
    pub output_dir: PathBuf,
}

#[derive(Debug, Clone)]
struct Row {
    symbol: String,
    exchange: String,
    time: String,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume: Option<f64>,
    turnover: Option<f64>,
    prev_close: Option<f64>,
}

pub fn run(args: Args) -> Result<()> {
    let files = collect_files(&args.input_dir, &["csv"], false)?;
    if files.is_empty() {
        return Err(anyhow!(
            "目录中未找到 csv 文件: {}",
            args.input_dir.display()
        ));
    }

    let mut groups: BTreeMap<(String, String, String), Vec<Row>> = BTreeMap::new();
    let mut scanned = 0usize;
    for (idx, path) in files.iter().enumerate() {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(open_file(path)?);
        let mut file_rows = 0usize;
        for record in reader.records() {
            if let Some(row) = parse_row(&record?) {
                groups
                    .entry((
                        row.exchange.clone(),
                        row.time[0..4].to_string(),
                        row.time[5..7].to_string(),
                    ))
                    .or_default()
                    .push(row);
                file_rows += 1;
            }
        }
        scanned += file_rows;
        println!(
            "[CSV] {}/{} 完成: {}, 解析 {} 条",
            idx + 1,
            files.len(),
            path.display(),
            file_rows
        );
    }

    let mut written = 0usize;
    let mut parts = 0usize;
    for ((exchange, year, month), rows) in groups {
        let mut deduped = dedup_rows(rows);
        deduped.sort_by(|a, b| a.time.cmp(&b.time).then(a.symbol.cmp(&b.symbol)));
        let out = args
            .output_dir
            .join("curated/index_daily_bars")
            .join(format!("exchange={exchange}"))
            .join(format!("year={year}"))
            .join(format!("month={month}"))
            .join("data.parquet");
        written += deduped.len();
        parts += 1;
        write_file(&out, to_parquet_bytes(&deduped)?)?;
    }
    println!(
        "[DONE] index-daily 完成: 扫描 {} 条, 写入 {} 条, {} 个 part",
        scanned, written, parts
    );
    Ok(())
}

fn parse_row(row: &StringRecord) -> Option<Row> {
    let symbol = row.get(0)?.trim();
    let time = row.get(1)?.trim();
    let exchange = symbol.split('.').nth(1)?.trim();
    if exchange.is_empty() || time.len() < 10 {
        return None;
    }
    Some(Row {
        symbol: symbol.to_string(),
        exchange: exchange.to_string(),
        time: time[0..10].to_string(),
        open: parse_opt_f64(row.get(2)),
        high: parse_opt_f64(row.get(3)),
        low: parse_opt_f64(row.get(4)),
        close: parse_opt_f64(row.get(5)),
        volume: parse_opt_f64(row.get(6)),
        turnover: parse_opt_f64(row.get(7)),
        prev_close: parse_opt_f64(row.get(9)),
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
        Field::new("open", DataType::Float64, true),
        Field::new("high", DataType::Float64, true),
        Field::new("low", DataType::Float64, true),
        Field::new("close", DataType::Float64, true),
        Field::new("volume", DataType::Float64, true),
        Field::new("turnover", DataType::Float64, true),
        Field::new("prev_close", DataType::Float64, true),
    ]));
    let mut symbol = StringBuilder::new();
    let mut exchange = StringBuilder::new();
    let mut time = StringBuilder::new();
    let mut open = Float64Builder::new();
    let mut high = Float64Builder::new();
    let mut low = Float64Builder::new();
    let mut close = Float64Builder::new();
    let mut volume = Float64Builder::new();
    let mut turnover = Float64Builder::new();
    let mut prev_close = Float64Builder::new();
    for row in rows {
        symbol.append_value(&row.symbol);
        exchange.append_value(&row.exchange);
        time.append_value(&row.time);
        open.append_option(row.open);
        high.append_option(row.high);
        low.append_option(row.low);
        close.append_option(row.close);
        volume.append_option(row.volume);
        turnover.append_option(row.turnover);
        prev_close.append_option(row.prev_close);
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
            Arc::new(turnover.finish()) as ArrayRef,
            Arc::new(prev_close.finish()) as ArrayRef,
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
