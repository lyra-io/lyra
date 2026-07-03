use chronicle_catalog::error::CatalogError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
}
