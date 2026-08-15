use crate::error_inner::InnerError;
use crate::xunit::error::XunitClientError;
use meta::error::MetadataError;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum LyraError {
    #[error("Stream not found: {0}")]
    StreamNotFound(String),

    #[error("Stream already exists: {0}")]
    StreamAlreadyExists(String),

    #[error("Invalid term: current={current}, requested={requested}")]
    InvalidTerm { current: i64, requested: i64 },

    #[error("Fenced: stream_id={stream_id}, term={term}")]
    Fenced { stream_id: i64, term: i64 },

    #[error("Reconciliation failed: {0}")]
    ReconciliationFailed(String),

    #[error("Unit not enough: {0}")]
    UnitNotEnough(String),

    #[error("Metadata error: {0}")]
    Metadata(#[from] MetadataError),

    #[error("XUnit error: {0}")]
    Xunit(#[from] XunitClientError),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Canceled")]
    Canceled,
}

impl From<tonic::Status> for LyraError {
    fn from(status: tonic::Status) -> Self {
        LyraError::Transport(status.to_string())
    }
}

impl From<InnerError> for LyraError {
    fn from(value: InnerError) -> Self {
        match value {
            InnerError::FenceFailed(message) => LyraError::ReconciliationFailed(message),
            InnerError::Transport(message) => LyraError::Transport(message),
            InnerError::InvalidTerm { expect, actual } => LyraError::InvalidTerm {
                current: actual,
                requested: expect,
            },
            InnerError::Metadata(error) => LyraError::Metadata(error),
            InnerError::UnitNotEnough(message) => LyraError::UnitNotEnough(message),
            InnerError::Canceled => LyraError::Canceled,
        }
    }
}
