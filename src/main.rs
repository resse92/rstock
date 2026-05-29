use anyhow::Result;
use rstock::server::{run_server, ServerConfig};

#[tokio::main]
async fn main() -> Result<()> {
    run_server(ServerConfig::from_file("config.toml")?).await
}
