//! Errors produced by WAL segment files.

use std::io::Error;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub(in crate::wal) enum SegmentError {
    #[error("segment I/O error: {0}")]
    Io(String),

    #[error("segment corruption in {path}: {message}")]
    Corruption { path: PathBuf, message: String },

    #[error("truncated segment record in {path}: {message}")]
    Truncated { path: PathBuf, message: String },

    #[error("segment number {0} exceeds the physical format limit")]
    SegmentNumberTooLarge(u64),

    #[error("encoded record size {size} exceeds the maximum segment size {max}")]
    RecordTooLarge { size: u64, max: u64 },

    #[error("segment offset space is exhausted")]
    OffsetExhausted,
}

impl From<Error> for SegmentError {
    fn from(error: Error) -> Self {
        Self::Io(error.to_string())
    }
}
