use anyhow::Result;
use rstock::server::{run_server, ServerConfig};
use rstock::telemetry::init_logging;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
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
