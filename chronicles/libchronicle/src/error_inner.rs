use chronicle_catalog::error::CatalogError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum InnerError {
    #[error("Fenced error: {0}")]
    FenceFailed(String),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Invalid term: {expect} (actual: {actual})")]
    InvalidTerm { expect: i64, actual: i64 },

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("Unit not enough: {0}")]
    UnitNotEnough(String),

    #[error("Canceled")]
    Canceled,
}

impl From<tonic::Status> for InnerError {
    fn from(status: tonic::Status) -> Self {
        InnerError::Transport(status.to_string())
    }
}
