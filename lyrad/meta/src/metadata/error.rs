use oxia::OxiaError;
use prost::DecodeError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, MetadataError>;

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("invalid metadata key {key:?}: {reason}")]
    InvalidKey { key: String, reason: &'static str },

    #[error("metadata write condition was not satisfied for key {0:?}")]
    Conflict(String),

    #[error("metadata counter {0:?} is exhausted")]
    CounterExhausted(String),

    #[error("failed to decode protobuf metadata: {0}")]
    Decode(#[from] DecodeError),

    #[error("Oxia metadata operation failed: {0}")]
    Oxia(#[source] OxiaError),
}

impl From<OxiaError> for MetadataError {
    fn from(error: OxiaError) -> Self {
        Self::Oxia(error)
    }
}
