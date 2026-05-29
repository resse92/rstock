use std::sync::Arc;

use tokio::sync::Mutex;

use super::config::ServerConfig;

#[derive(Clone)]
pub struct AppState {
    pub args: Arc<ServerConfig>,
    pub sync_lock: Arc<Mutex<()>>,
}

impl AppState {
    pub fn new(args: ServerConfig) -> Self {
        Self {
            args: Arc::new(args),
            sync_lock: Arc::new(Mutex::new(())),
        }
    }
}
