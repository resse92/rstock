use anyhow::Result;
use clap::{Parser, Subcommand};
use dotenvy::dotenv;
use rust_kline_fetcher::import_daily::{run_import_daily, ImportDailyArgs};
use rust_kline_fetcher::import_index_csv::{run_import_index_csv, ImportIndexCsvArgs};
use rust_kline_fetcher::import_minute::{run_import_minute, ImportMinuteArgs};
use rust_kline_fetcher::sync_daily::{run_sync_daily, SyncDailyArgs};

#[derive(Debug, Parser)]
#[command(name = "kline-sync", version, about = "A股K线同步命令")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// 按日期区间下载日线并写入 S3 parquet
    SyncDaily(SyncDailyArgs),
    /// 从本地目录导入日线 zip/csv 并写入 S3 parquet
    ImportDaily(ImportDailyArgs),
    /// 从本地目录导入指数 CSV 并写入 S3 parquet
    ImportIndexCsv(ImportIndexCsvArgs),
    /// 从本地目录递归导入分钟 ZIP 并写入 S3 parquet
    ImportMinute(ImportMinuteArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let cli = Cli::parse();

    match cli.command {
        Commands::SyncDaily(args) => run_sync_daily(args).await,
        Commands::ImportDaily(args) => run_import_daily(args).await,
        Commands::ImportIndexCsv(args) => run_import_index_csv(args).await,
        Commands::ImportMinute(args) => run_import_minute(args).await,
    }
}
