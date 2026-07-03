pub mod error;

use chronicle_catalog::{Action, ActionRequest, CatalogRef, Versioned};
use error::XunitError;
use libxunit::{
    AppendRowsRequest, AppendRowsResponse, ScanRequest, ScanResponse, XunitClient,
    error::XunitClientError,
};

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

#[async_trait::async_trait]
impl XunitClient for Xunit {
    async fn append_rows(
        &self,
        _request: AppendRowsRequest,
    ) -> Result<AppendRowsResponse, XunitClientError> {
        Err(XunitClientError::Internal(
            "xunit append_rows is not implemented yet".into(),
        ))
    }

    async fn scan(&self, _request: ScanRequest) -> Result<ScanResponse, XunitClientError> {
        Err(XunitClientError::Internal(
            "xunit scan is not implemented yet".into(),
        ))
    }

    async fn submit_action(
        &self,
        request: ActionRequest,
    ) -> Result<Versioned<Action>, XunitClientError> {
        self.catalog
            .submit_action(request)
            .await
            .map_err(|error| XunitClientError::Internal(error.to_string()))
    }
}
