use std::path::PathBuf;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum WalError {
    #[error("invalid WAL options: {0}")]
    InvalidOptions(String),

    #[error("WAL is closed")]
    Closed,

    #[error("WAL I/O error: {0}")]
    Io(String),

    #[error("WAL corruption in {path}: {message}")]
    Corruption { path: PathBuf, message: String },

    #[error("WAL sequence {requested} has expired; earliest available sequence is {earliest}")]
    SequenceExpired { requested: u64, earliest: u64 },

    #[error("WAL payload is too large: {actual} bytes (maximum {maximum})")]
    PayloadTooLarge { actual: usize, maximum: usize },

    #[error("WAL worker failed: {0}")]
    Worker(String),
}

impl From<std::io::Error> for WalError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}
