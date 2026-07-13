pub mod dataset;
pub mod error;
pub mod memory_catalog;
pub mod oxia_catalog;
pub mod types;

use async_trait::async_trait;
use error::CatalogError;
use lyra_proto::pb_catalog::{Segment, StreamMeta, UnitRegistration};
use oxia_catalog::OxiaCatalog;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tracing::info;

pub use dataset::*;
pub use memory_catalog::{MemoryCatalog, build_memory_catalog};
pub use types::*;

/// Wraps a value with its catalog version for CAS operations.
#[derive(Debug, Clone)]
pub struct Versioned<T> {
    pub value: T,
    pub version: i64,
}

impl<T> Versioned<T> {
    pub fn new(value: T, version: i64) -> Self {
        Self { value, version }
    }
}

pub const SEGMENT_KEY_PAD: usize = 19;

#[async_trait]
pub trait Catalog: Send + Sync {
    async fn create_dataset(&self, _dataset: Dataset) -> Result<Versioned<Dataset>, CatalogError> {
        Err(CatalogError::Unsupported("create_dataset".into()))
    }

    async fn update_dataset(
        &self,
        _dataset: Dataset,
        _expected_version: i64,
    ) -> Result<Versioned<Dataset>, CatalogError> {
        Err(CatalogError::Unsupported("update_dataset".into()))
    }

    async fn get_dataset(&self, _name: &str) -> Result<Versioned<Dataset>, CatalogError> {
        Err(CatalogError::Unsupported("get_dataset".into()))
    }

    async fn list_datasets(&self) -> Result<Vec<Versioned<Dataset>>, CatalogError> {
        Err(CatalogError::Unsupported("list_datasets".into()))
    }

    async fn delete_dataset(
        &self,
        _name: &str,
        _expected_version: i64,
    ) -> Result<(), CatalogError> {
        Err(CatalogError::Unsupported("delete_dataset".into()))
    }

    async fn submit_action(
        &self,
        _request: ActionRequest,
    ) -> Result<Versioned<Action>, CatalogError> {
        Err(CatalogError::Unsupported("submit_action".into()))
    }

    async fn get_action(&self, _id: &ActionId) -> Result<Versioned<Action>, CatalogError> {
        Err(CatalogError::Unsupported("get_action".into()))
    }

    async fn list_actions(
        &self,
        _dataset: Option<&DatasetName>,
    ) -> Result<Vec<Versioned<Action>>, CatalogError> {
        Err(CatalogError::Unsupported("list_actions".into()))
    }

    async fn get_stream(&self, name: &str) -> Result<StreamMeta, CatalogError>;

    async fn stream_update(
        &self,
        meta: &StreamMeta,
        expected_version: i64,
    ) -> Result<StreamMeta, CatalogError>;

    async fn create_stream(&self, name: &str) -> Result<StreamMeta, CatalogError>;

    async fn delete_stream(&self, name: &str, expected_version: i64) -> Result<(), CatalogError>;

    async fn list_streams(&self) -> Result<Vec<StreamMeta>, CatalogError>;

    async fn put_segment(
        &self,
        stream_name: &str,
        segment: &Segment,
        expected_version: i64,
    ) -> Result<Versioned<Segment>, CatalogError>;

    async fn list_segments(
        &self,
        stream_name: &str,
    ) -> Result<Vec<Versioned<Segment>>, CatalogError>;

    async fn get_last_segment(
        &self,
        stream_name: &str,
    ) -> Result<Option<Versioned<Segment>>, CatalogError>;

    async fn get_segment_for_offset(
        &self,
        stream_name: &str,
        offset: i64,
    ) -> Result<Option<Versioned<Segment>>, CatalogError>;

    async fn stream_get_or_insert(&self, name: &str) -> Result<StreamMeta, CatalogError>;

    async fn stream_new_term(&self, name: &str) -> Result<StreamMeta, CatalogError>;

    async fn register_unit(&self, registration: &UnitRegistration) -> Result<(), CatalogError>;

    async fn unregister_unit(&self, address: &str, zone: &str) -> Result<(), CatalogError>;

    async fn list_units(&self) -> Result<Vec<UnitRegistration>, CatalogError>;

    async fn list_writable_units(&self) -> Result<Vec<UnitRegistration>, CatalogError>;

    async fn subscribe_segments(&self, stream_name: &str)
    -> Result<Receiver<String>, CatalogError>;
}

pub type CatalogRef = Arc<dyn Catalog>;

pub fn segment_key(name: &str, start_offset: i64) -> String {
    format!(
        "/lyra/streams/{}/seg-{:0>width$}",
        name,
        start_offset,
        width = SEGMENT_KEY_PAD
    )
}

pub fn segment_key_prefix(name: &str) -> String {
    format!("/lyra/streams/{}/seg-", name)
}

pub fn segment_key_max(name: &str) -> String {
    format!("/lyra/streams/{}/seg-{}", name, "9".repeat(SEGMENT_KEY_PAD))
}

#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CatalogOptions {
    #[serde(default = "default_service_address")]
    pub service_address: String,
    #[serde(default = "default_namespace")]
    pub namespace: String,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self {
            service_address: default_service_address(),
            namespace: default_namespace(),
        }
    }
}

fn default_service_address() -> String {
    "localhost:6648".to_string()
}

fn default_namespace() -> String {
    "default".to_string()
}

pub async fn build_catalog(options: &CatalogOptions) -> Result<CatalogRef, CatalogError> {
    Ok(Arc::new(build_oxia_catalog(options).await?))
}

pub async fn build_oxia_catalog(options: &CatalogOptions) -> Result<OxiaCatalog, CatalogError> {
    info!(
        address = %options.service_address,
        namespace = %options.namespace,
        "connecting to oxia catalog"
    );
    OxiaCatalog::new(options.service_address.clone(), options.namespace.clone()).await
}
