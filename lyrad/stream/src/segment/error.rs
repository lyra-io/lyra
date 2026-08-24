//! Errors produced by stream storage segments.

use std::{io::Error, path::PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error("segment I/O error: {0}")]
    Io(String),

    #[error("segment corruption in {path}: {message}")]
    Corruption { path: PathBuf, message: String },

    #[error("segment number {0} exceeds the physical format limit")]
    SegmentNumberTooLarge(u64),

    #[error("cannot append to a sealed segment")]
    Sealed,

    #[error("encoded record size {size} exceeds the maximum record area size {max}")]
    RecordTooLarge { size: u64, max: u64 },

    #[error("segment offset space is exhausted")]
    OffsetExhausted,
}

impl From<Error> for SegmentError {
    fn from(error: Error) -> Self {
        Self::Io(error.to_string())
    }
}
