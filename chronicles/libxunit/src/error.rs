use thiserror::Error;

#[derive(Debug, Error)]
pub enum XunitClientError {
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    #[error("Internal xunit error: {0}")]
    Internal(String),
}
