//! Write-ahead log configuration.

use std::path::PathBuf;

/// Configuration for opening a [`Log`](super::Log).
#[derive(Debug, Clone)]
pub struct LogOptions {
    /// Directory where segment files are stored and recovered from.
    pub dir: PathBuf,

    /// Whether append acknowledgements wait for WAL synchronization.
    pub sync: bool,
}

impl LogOptions {
    pub fn new(dir: impl Into<PathBuf>, sync: bool) -> Self {
        Self {
            dir: dir.into(),
            sync,
        }
    }
}

impl Default for LogOptions {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("/tmp/lyra-wal"),
            sync: true,
        }
    }
}
