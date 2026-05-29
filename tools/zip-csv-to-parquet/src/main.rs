mod common;
mod config;
mod daily;
mod index_daily;
mod minute;
mod minute_s3;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "zip-csv-to-parquet",
    version,
    about = "本地 ZIP/CSV 转 Parquet 工具"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// 股票日线 ZIP/CSV 转本地 Parquet
    Daily(daily::Args),
    /// 指数日线 CSV 转本地 Parquet
    IndexDaily(index_daily::Args),
    /// 分钟线 ZIP/CSV 转本地 Parquet
    Minute(minute::Args),
    /// 分钟线 ZIP/CSV 转远端 S3 Parquet
    MinuteS3(minute_s3::Args),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Daily(args) => daily::run(args),
        Commands::IndexDaily(args) => index_daily::run(args),
        Commands::Minute(args) => minute::run(args),
        Commands::MinuteS3(args) => minute_s3::run(args).await,
    }
}
