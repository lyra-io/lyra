use chronicle_catalog::error::CatalogError;
use libxunit::error::XunitClientError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum XunitError {
    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
    #[error("Client error: {0}")]
    Client(#[from] XunitClientError),
}
