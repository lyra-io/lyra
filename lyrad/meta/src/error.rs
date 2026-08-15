use liboxia::errors::OxiaError;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum MetadataError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Version conflict: expected {expected}, got {actual}")]
    VersionConflict { expected: i64, actual: i64 },
    #[error("Transport error: {0}")]
    Transport(String),
    #[error("Already exists: {0}")]
    AlreadyExists(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Unsupported metadata operation: {0}")]
    Unsupported(String),
}

impl From<OxiaError> for MetadataError {
    fn from(err: OxiaError) -> Self {
        match err {
            OxiaError::KeyNotFound() => MetadataError::NotFound("key not found".to_string()),
            OxiaError::UnexpectedVersionId() => MetadataError::VersionConflict {
                expected: -1,
                actual: -1,
            },
            OxiaError::Transport(msg) => MetadataError::Transport(msg),
            other => MetadataError::Internal(other.to_string()),
        }
    }
}
