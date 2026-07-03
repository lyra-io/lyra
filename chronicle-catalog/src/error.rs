use liboxia::errors::OxiaError;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum CatalogError {
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
}

impl From<OxiaError> for CatalogError {
    fn from(err: OxiaError) -> Self {
        match err {
            OxiaError::KeyNotFound() => CatalogError::NotFound("key not found".to_string()),
            OxiaError::UnexpectedVersionId() => CatalogError::VersionConflict {
                expected: -1,
                actual: -1,
            },
            OxiaError::Transport(msg) => CatalogError::Transport(msg),
            other => CatalogError::Internal(other.to_string()),
        }
    }
}
