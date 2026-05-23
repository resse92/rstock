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

use crate::common::{collect_files, open_file, output_id, parse_opt_f64, write_file};

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[arg(long, help = "股票日线 zip/csv 目录")]
    pub input_dir: PathBuf,
    #[arg(long, help = "Parquet 输出根目录")]
    pub output_dir: PathBuf,
}

#[derive(Debug, Clone)]
struct Row {
    symbol: String,
    time: String,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    volume: Option<f64>,
    turnover: Option<f64>,
    factor: Option<f64>,
    prev_close: Option<f64>,
    avg_price: Option<f64>,
    high_limit: Option<f64>,
    low_limit: Option<f64>,
    turnover_rate: Option<f64>,
    amp_rate: Option<f64>,
    quote_rate: Option<f64>,
    is_paused: Option<f64>,
    is_st: Option<f64>,
}

pub fn run(args: Args) -> Result<()> {
    let files = collect_files(&args.input_dir, &["zip", "csv"], false)?;
    if files.is_empty() {
        return Err(anyhow!(
            "目录中未找到 zip 或 csv 文件: {}",
            args.input_dir.display()
        ));
    }

    let mut total_rows = 0usize;
    let mut total_parts = 0usize;
    for (idx, path) in files.iter().enumerate() {
        let groups =
            parse_source(path).with_context(|| format!("解析源文件失败: {}", path.display()))?;
        let file_id = output_id(path, &args.input_dir, idx);
        let mut file_rows = 0usize;
        let mut file_parts = 0usize;
        for ((exchange, year, month), mut rows) in groups {
            rows.sort_by(|a, b| a.time.cmp(&b.time).then(a.symbol.cmp(&b.symbol)));
            let out = args
                .output_dir
                .join("curated/daily_bars")
                .join(format!("exchange={exchange}"))
                .join(format!("year={year}"))
                .join(format!("month={month}"))
                .join(format!("{file_id}.parquet"));
            file_rows += rows.len();
            file_parts += 1;
            write_file(&out, to_parquet_bytes(&rows)?)?;
        }
        total_rows += file_rows;
        total_parts += file_parts;
        println!(
            "[FILE] {}/{} 完成: {}, 写入 {} 条, {} 个 part",
            idx + 1,
            files.len(),
            path.display(),
            file_rows,
            file_parts
        );
    }
    println!(
        "[DONE] daily 完成: 写入 {} 条, {} 个 part",
        total_rows, total_parts
    );
    Ok(())
}

fn parse_source(path: &Path) -> Result<BTreeMap<(String, String, String), Vec<Row>>> {
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or_default();
    if ext.eq_ignore_ascii_case("zip") {
        return parse_zip(path);
    }
    if ext.eq_ignore_ascii_case("csv") {
        return parse_csv(path);
    }
    Err(anyhow!("不支持的文件类型: {}", path.display()))
}

fn parse_zip(path: &Path) -> Result<BTreeMap<(String, String, String), Vec<Row>>> {
    let mut groups = BTreeMap::new();
    let mut archive = ZipArchive::new(open_file(path)?)?;
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
        for row in reader.records() {
            add_row(&mut groups, row?)?;
        }
    }
    Ok(groups)
}

fn parse_csv(path: &Path) -> Result<BTreeMap<(String, String, String), Vec<Row>>> {
    let mut groups = BTreeMap::new();
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(open_file(path)?);
    for row in reader.records() {
        add_row(&mut groups, row?)?;
    }
    Ok(groups)
}

fn add_row(
    groups: &mut BTreeMap<(String, String, String), Vec<Row>>,
    record: StringRecord,
) -> Result<()> {
    if let Some(row) = parse_row(&record) {
        let exchange = row.symbol.split('.').nth(1).unwrap_or_default().to_string();
        groups
            .entry((
                exchange,
                row.time[0..4].to_string(),
                row.time[5..7].to_string(),
            ))
            .or_default()
            .push(row);
    }
    Ok(())
}

fn parse_row(row: &StringRecord) -> Option<Row> {
    let symbol = row.get(0)?.trim();
    let time = row.get(1)?.trim();
    symbol.split('.').nth(1)?;
    if time.len() < 10 {
        return None;
    }
    Some(Row {
        symbol: symbol.to_string(),
        time: time[0..10].to_string(),
        open: parse_opt_f64(row.get(2)),
        high: parse_opt_f64(row.get(3)),
        low: parse_opt_f64(row.get(4)),
        close: parse_opt_f64(row.get(5)),
        volume: parse_opt_f64(row.get(6)),
        turnover: parse_opt_f64(row.get(7)),
        factor: parse_opt_f64(row.get(8)),
        prev_close: parse_opt_f64(row.get(9)),
        avg_price: parse_opt_f64(row.get(10)),
        high_limit: parse_opt_f64(row.get(11)),
        low_limit: parse_opt_f64(row.get(12)),
        turnover_rate: parse_opt_f64(row.get(13)),
        amp_rate: parse_opt_f64(row.get(14)),
        quote_rate: parse_opt_f64(row.get(15)),
        is_paused: parse_opt_f64(row.get(16)),
        is_st: parse_opt_f64(row.get(17)),
    })
}

fn to_parquet_bytes(rows: &[Row]) -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("time", DataType::Utf8, false),
        Field::new("open", DataType::Float64, true),
        Field::new("high", DataType::Float64, true),
        Field::new("low", DataType::Float64, true),
        Field::new("close", DataType::Float64, true),
        Field::new("volume", DataType::Float64, true),
        Field::new("turnover", DataType::Float64, true),
        Field::new("factor", DataType::Float64, true),
        Field::new("prev_close", DataType::Float64, true),
        Field::new("avg_price", DataType::Float64, true),
        Field::new("high_limit", DataType::Float64, true),
        Field::new("low_limit", DataType::Float64, true),
        Field::new("turnover_rate", DataType::Float64, true),
        Field::new("amp_rate", DataType::Float64, true),
        Field::new("quote_rate", DataType::Float64, true),
        Field::new("is_paused", DataType::Float64, true),
        Field::new("is_st", DataType::Float64, true),
    ]));
    let mut symbol = StringBuilder::new();
    let mut time = StringBuilder::new();
    let mut open = Float64Builder::new();
    let mut high = Float64Builder::new();
    let mut low = Float64Builder::new();
    let mut close = Float64Builder::new();
    let mut volume = Float64Builder::new();
    let mut turnover = Float64Builder::new();
    let mut factor = Float64Builder::new();
    let mut prev_close = Float64Builder::new();
    let mut avg_price = Float64Builder::new();
    let mut high_limit = Float64Builder::new();
    let mut low_limit = Float64Builder::new();
    let mut turnover_rate = Float64Builder::new();
    let mut amp_rate = Float64Builder::new();
    let mut quote_rate = Float64Builder::new();
    let mut is_paused = Float64Builder::new();
    let mut is_st = Float64Builder::new();
    for row in rows {
        symbol.append_value(&row.symbol);
        time.append_value(&row.time);
        open.append_option(row.open);
        high.append_option(row.high);
        low.append_option(row.low);
        close.append_option(row.close);
        volume.append_option(row.volume);
        turnover.append_option(row.turnover);
        factor.append_option(row.factor);
        prev_close.append_option(row.prev_close);
        avg_price.append_option(row.avg_price);
        high_limit.append_option(row.high_limit);
        low_limit.append_option(row.low_limit);
        turnover_rate.append_option(row.turnover_rate);
        amp_rate.append_option(row.amp_rate);
        quote_rate.append_option(row.quote_rate);
        is_paused.append_option(row.is_paused);
        is_st.append_option(row.is_st);
    }
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(symbol.finish()) as ArrayRef,
            Arc::new(time.finish()) as ArrayRef,
            Arc::new(open.finish()) as ArrayRef,
            Arc::new(high.finish()) as ArrayRef,
            Arc::new(low.finish()) as ArrayRef,
            Arc::new(close.finish()) as ArrayRef,
            Arc::new(volume.finish()) as ArrayRef,
            Arc::new(turnover.finish()) as ArrayRef,
            Arc::new(factor.finish()) as ArrayRef,
            Arc::new(prev_close.finish()) as ArrayRef,
            Arc::new(avg_price.finish()) as ArrayRef,
            Arc::new(high_limit.finish()) as ArrayRef,
            Arc::new(low_limit.finish()) as ArrayRef,
            Arc::new(turnover_rate.finish()) as ArrayRef,
            Arc::new(amp_rate.finish()) as ArrayRef,
            Arc::new(quote_rate.finish()) as ArrayRef,
            Arc::new(is_paused.finish()) as ArrayRef,
            Arc::new(is_st.finish()) as ArrayRef,
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
