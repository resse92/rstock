use anyhow::Result;
use rstock::server::{run_server, ServerConfig};
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
    run_server(ServerConfig::from_file("config.toml")?).await
}
