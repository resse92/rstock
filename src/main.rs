use anyhow::Result;
use clap::{Parser, Subcommand};
use dotenvy::dotenv;
use rstock::import_minute::{run_import_minute, ImportMinuteArgs};
use rstock::server::{run_server, ServerConfig};
use rstock::sync_daily::{run_sync_daily, SyncDailyArgs};
use rstock::sync_minute::{run_sync_minute, SyncMinuteArgs};

#[derive(Debug, Parser)]
#[command(name = "rstock", version, about = "A股行情数据工具")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// 按日期区间下载日线并写入 S3 parquet
    SyncDaily(SyncDailyArgs),
    /// 从本地目录递归导入分钟 ZIP/CSV 并写入 S3 parquet
    ImportMinute(ImportMinuteArgs),
    /// 在线同步 1 分钟线并写入 S3 parquet
    SyncMinute(SyncMinuteArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::SyncDaily(args)) => run_sync_daily(args).await,
        Some(Commands::ImportMinute(args)) => run_import_minute(args).await,
        Some(Commands::SyncMinute(args)) => run_sync_minute(args).await,
        None => run_server(ServerConfig::from_env()?).await,
    }
}
