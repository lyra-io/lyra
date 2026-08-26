//! Errors produced by the stream storage write-ahead log.

use std::io::Error;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum WalError {
    /// The log was closed or is no longer accepting appends.
    #[error("WAL is closed")]
    Closed,

    /// An operating-system level I/O failure.
    #[error("WAL I/O error: {0}")]
    Io(String),

    /// Another log instance owns the WAL directory.
    #[error("WAL directory is already in use: {0}")]
    Locked(PathBuf),

    /// On-disk data failed validation and cannot be safely recovered.
    #[error("WAL corruption in {path}: {message}")]
    Corruption { path: PathBuf, message: String },

    /// A physical WAL record is incomplete.
    #[error("truncated WAL record in {path}: {message}")]
    Truncated { path: PathBuf, message: String },

    /// A segment number exceeds the physical record format.
    #[error("WAL segment number {0} exceeds the physical format limit")]
    SegmentNumberTooLarge(u64),

    /// An encoded record cannot fit in an empty segment.
    #[error("encoded record size {size} exceeds the maximum WAL segment size {max}")]
    RecordTooLarge { size: u64, max: u64 },

    /// The caller's read limit cannot hold the first record.
    #[error("WAL record payload size {size} exceeds the maximum read size {max}")]
    ReadBufferTooSmall { size: usize, max: usize },

    /// A physical WAL position cannot be represented.
    #[error("WAL position space is exhausted")]
    PositionExhausted,

    /// An internal background worker failed.
    #[error("WAL worker failed: {0}")]
    Worker(String),
}

impl WalError {
    pub(crate) fn corruption(path: &Path, message: impl Into<String>) -> Self {
        Self::Corruption {
            path: path.to_path_buf(),
            message: message.into(),
        }
    }

    pub(crate) fn truncated(path: &Path, message: impl Into<String>) -> Self {
        Self::Truncated {
            path: path.to_path_buf(),
            message: message.into(),
        }
    }
}

impl From<Error> for WalError {
    fn from(error: Error) -> Self {
        Self::Io(error.to_string())
    }
}
