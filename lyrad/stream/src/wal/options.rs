//! Write-ahead log configuration.

use std::path::PathBuf;

/// Configuration for opening a [`Log`](super::Log).
#[derive(Debug, Clone)]
pub struct LogOptions {
    /// Directory where segment files are stored and recovered from.
    pub dir: PathBuf,
}

impl LogOptions {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }
}

impl Default for LogOptions {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("/tmp/lyra-wal"),
        }
    }
}
