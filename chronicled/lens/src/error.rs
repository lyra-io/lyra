use chronicle_catalog::error::CatalogError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LensError {
    #[error("Unsupported SQL statement: {0}")]
    UnsupportedStatement(String),
    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
}
