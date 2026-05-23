mod common;
mod daily;
mod index_daily;
mod minute;

use anyhow::Result;
use clap::{Parser, Subcommand};
use dotenvy::dotenv;

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
}

fn main() -> Result<()> {
    dotenv().ok();
    let cli = Cli::parse();
    match cli.command {
        Commands::Daily(args) => daily::run(args),
        Commands::IndexDaily(args) => index_daily::run(args),
        Commands::Minute(args) => minute::run(args),
    }
}
