use datafusion::error::DataFusionError;
use meta::metadata::MetadataError;
use std::io;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, CataError>;

#[derive(Debug, Error)]
pub enum CataError {
    #[error("failed to plan SQL: {0}")]
    Plan(#[from] DataFusionError),

    #[error("failed to configure the PostgreSQL catalog: {0}")]
    QueryCatalog(#[from] Box<DataFusionError>),

    #[error(transparent)]
    Metadata(#[from] MetadataError),

    #[error("PostgreSQL server failed: {0}")]
    Server(#[from] io::Error),
}
