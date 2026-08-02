use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum SegmentError {
    #[error("segment I/O error: {0}")]
    Io(String),

    #[error("segment corruption in {path}: {message}")]
    Corruption { path: PathBuf, message: String },

    #[error("segment number {0} exceeds the physical format limit")]
    SegmentNumberTooLarge(u64),
}

impl From<std::io::Error> for SegmentError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}
