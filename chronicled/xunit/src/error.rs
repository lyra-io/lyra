use chronicle_catalog::error::CatalogError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum XunitError {
    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
}
