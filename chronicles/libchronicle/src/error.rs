use crate::error_inner::InnerError;
use chronicle_catalog::error::CatalogError;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum ChronicleError {
    #[error("Timeline not found: {0}")]
    TimelineNotFound(String),

    #[error("Timeline already exists: {0}")]
    TimelineAlreadyExists(String),

    #[error("Invalid term: current={current}, requested={requested}")]
    InvalidTerm { current: i64, requested: i64 },

    #[error("Fenced: timeline_id={timeline_id}, term={term}")]
    Fenced { timeline_id: i64, term: i64 },

    #[error("Reconciliation failed: {0}")]
    ReconciliationFailed(String),

    #[error("Unit not enough: {0}")]
    UnitNotEnough(String),

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Canceled")]
    Canceled,
}

impl From<tonic::Status> for ChronicleError {
    fn from(status: tonic::Status) -> Self {
        ChronicleError::Transport(status.to_string())
    }
}

impl From<InnerError> for ChronicleError {
    fn from(value: InnerError) -> Self {
        match value {
            InnerError::FenceFailed(message) => ChronicleError::ReconciliationFailed(message),
            InnerError::Transport(message) => ChronicleError::Transport(message),
            InnerError::InvalidTerm { expect, actual } => ChronicleError::InvalidTerm {
                current: actual,
                requested: expect,
            },
            InnerError::Catalog(error) => ChronicleError::Catalog(error),
            InnerError::UnitNotEnough(message) => ChronicleError::UnitNotEnough(message),
            InnerError::Canceled => ChronicleError::Canceled,
        }
    }
}
