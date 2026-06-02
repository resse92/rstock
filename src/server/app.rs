use std::sync::Arc;

use tokio::sync::Mutex;

use super::whole_quote::WholeQuoteSubscription;

use super::config::ServerConfig;

#[derive(Clone)]
pub struct AppState {
    pub args: Arc<ServerConfig>,
    pub sync_lock: Arc<Mutex<()>>,
    pub whole_quote: Arc<Mutex<WholeQuoteSubscription>>,
}

impl AppState {
    pub fn new(args: ServerConfig) -> Self {
        Self {
            args: Arc::new(args),
            sync_lock: Arc::new(Mutex::new(())),
            whole_quote: Arc::new(Mutex::new(WholeQuoteSubscription::default())),
        }
    }
}
