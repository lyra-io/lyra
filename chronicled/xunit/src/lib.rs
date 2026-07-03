pub mod error;

use chronicle_catalog::{Action, ActionRequest, CatalogRef, Versioned};
use error::XunitError;

pub struct Xunit {
    catalog: CatalogRef,
}

impl Xunit {
    pub fn new(catalog: CatalogRef) -> Self {
        Self { catalog }
    }

    pub async fn submit_action(
        &self,
        request: ActionRequest,
    ) -> Result<Versioned<Action>, XunitError> {
        Ok(self.catalog.submit_action(request).await?)
    }
}
