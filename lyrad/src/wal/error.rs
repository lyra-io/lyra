use std::path::PathBuf;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum WalError {
    /// The supplied [`WalOptions`](super::WalOptions) failed validation.
    #[error("invalid WAL options: {0}")]
    InvalidOptions(String),

    /// The WAL was shut down or is no longer accepting appends.
    #[error("WAL is closed")]
    Closed,

    /// An operating-system level I/O failure.
    #[error("WAL I/O error: {0}")]
    Io(String),

    /// On-disk data failed validation and cannot be safely recovered.
    #[error("WAL corruption in {path}: {message}")]
    Corruption { path: PathBuf, message: String },

    /// `requested` has been trimmed; only `earliest` and later are available.
    #[error("WAL sequence {requested} has expired; earliest available sequence is {earliest}")]
    SequenceExpired { requested: u64, earliest: u64 },

    /// An internal background worker failed.
    #[error("WAL worker failed: {0}")]
    Worker(String),
}

impl From<std::io::Error> for WalError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<crate::segment::SegmentError> for WalError {
    fn from(error: crate::segment::SegmentError) -> Self {
        match error {
            crate::segment::SegmentError::Io(message) => Self::Io(message),
            crate::segment::SegmentError::Corruption { path, message } => {
                Self::Corruption { path, message }
            }
            crate::segment::SegmentError::SegmentNumberTooLarge(_) => {
                Self::Worker(error.to_string())
            }
        }
    }
}
