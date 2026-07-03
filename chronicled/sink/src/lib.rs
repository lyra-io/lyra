pub mod error;

use chronicle_catalog::CatalogRef;
use error::SinkError;
use tracing::info;

#[derive(Debug, Clone)]
pub struct SinkOptions {
    pub worker_id: String,
}

impl Default for SinkOptions {
    fn default() -> Self {
        Self {
            worker_id: "sink-0".to_string(),
        }
    }
}

pub struct Sink {
    catalog: CatalogRef,
    options: SinkOptions,
}

impl Sink {
    pub fn new(catalog: CatalogRef, options: SinkOptions) -> Self {
        Self { catalog, options }
    }

    pub async fn start(&self) -> Result<(), SinkError> {
        let _ = &self.catalog;
        info!(worker_id = %self.options.worker_id, "sink starting");
        Ok(())
    }
}
