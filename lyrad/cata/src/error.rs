use datafusion::error::DataFusionError;
use std::io;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, CataError>;

#[derive(Debug, Error)]
pub enum CataError {
    #[error("failed to configure the PostgreSQL catalog: {0}")]
    Catalog(#[from] Box<DataFusionError>),

    #[error("PostgreSQL server failed: {0}")]
    Server(#[from] io::Error),
}
