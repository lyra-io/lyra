//! Write-ahead log configuration.

use crate::segment::IoMode;
use std::path::PathBuf;

/// Configuration for opening a [`Log`](super::Log).
#[derive(Debug, Clone)]
pub struct LogOptions {
    /// Directory where segment files are stored and recovered from.
    pub dir: PathBuf,
    /// I/O mode used for segment reads and writes.
    pub io_mode: IoMode,
}

impl LogOptions {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            ..Self::default()
        }
    }
}

impl Default for LogOptions {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("/tmp/lyra-wal"),
            io_mode: IoMode::DirectPreferred,
        }
    }
}
