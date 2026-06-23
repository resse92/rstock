use anyhow::Result;
use rstock::server::{run_server, ServerConfig};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,rstock=info,rstock_jobs=info")),
        )
        .with_target(true)
        .init();
    let config_path = config_path()?;
    tracing::info!(config_path = %config_path.display(), "loading server config");
    run_server(ServerConfig::from_file(config_path)?).await
}

fn config_path() -> Result<PathBuf> {
    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" || arg == "-c" {
            let path = args
                .next()
                .ok_or_else(|| anyhow::anyhow!("--config requires a path"))?;
            return Ok(path.into());
        }
    }

    if let Some(path) = std::env::var_os("RSTOCK_CONFIG") {
        return Ok(path.into());
    }

    Ok(PathBuf::from("config.toml"))
}
