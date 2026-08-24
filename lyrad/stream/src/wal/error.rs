//! Errors produced by the stream storage write-ahead log.

use crate::segment;
use segment::SegmentError;
use std::{io::Error, path::PathBuf};
use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum LogError {
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

    /// An internal background worker failed.
    #[error("WAL worker failed: {0}")]
    Worker(String),
}

impl LogError {}

impl From<Error> for LogError {
    fn from(error: Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<SegmentError> for LogError {
    fn from(error: SegmentError) -> Self {
        match error {
            SegmentError::Io(message) => Self::Io(message),
            SegmentError::Corruption { path, message } => Self::Corruption { path, message },
            SegmentError::SegmentNumberTooLarge(_)
            | SegmentError::Sealed
            | SegmentError::RecordTooLarge { .. }
            | SegmentError::OffsetExhausted => Self::Worker(error.to_string()),
        }
    }
}
