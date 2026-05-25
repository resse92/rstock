use thiserror::Error;

#[derive(Error, Debug)]
pub enum TdxError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Connection timeout")]
    ConnectionTimeout,

    #[error("Setup handshake failed: {0}")]
    SetupFailed(String),

    #[error("Response parse error: {0}")]
    ResponseParse(String),

    #[error("Server disconnected")]
    Disconnected,

    #[error("Retry exhausted after {0} attempts")]
    RetryExhausted(usize),
}

pub type Result<T> = std::result::Result<T, TdxError>;
