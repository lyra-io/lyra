pub mod dataset;
pub mod error;
pub mod memory_metadata;
pub mod oxia_metadata;
pub mod proto;
pub mod types;

use async_trait::async_trait;
use error::MetadataError;
use crate::proto::pb_catalog::{Segment, StreamMeta, UnitRegistration};
use oxia_metadata::OxiaMetadata;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tracing::info;

pub use dataset::*;
pub use memory_metadata::{MemoryMetadata, build_memory_metadata};
pub use types::*;

/// Wraps a value with its metadata version for CAS operations.
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
pub trait Metadata: Send + Sync {
    async fn create_dataset(&self, _dataset: Dataset) -> Result<Versioned<Dataset>, MetadataError> {
        Err(MetadataError::Unsupported("create_dataset".into()))
    }

    async fn update_dataset(
        &self,
        _dataset: Dataset,
        _expected_version: i64,
    ) -> Result<Versioned<Dataset>, MetadataError> {
        Err(MetadataError::Unsupported("update_dataset".into()))
    }

    async fn get_dataset(&self, _name: &str) -> Result<Versioned<Dataset>, MetadataError> {
        Err(MetadataError::Unsupported("get_dataset".into()))
    }

    async fn list_datasets(&self) -> Result<Vec<Versioned<Dataset>>, MetadataError> {
        Err(MetadataError::Unsupported("list_datasets".into()))
    }

    async fn delete_dataset(
        &self,
        _name: &str,
        _expected_version: i64,
    ) -> Result<(), MetadataError> {
        Err(MetadataError::Unsupported("delete_dataset".into()))
    }

    async fn submit_action(
        &self,
        _request: ActionRequest,
    ) -> Result<Versioned<Action>, MetadataError> {
        Err(MetadataError::Unsupported("submit_action".into()))
    }

    async fn get_action(&self, _id: &ActionId) -> Result<Versioned<Action>, MetadataError> {
        Err(MetadataError::Unsupported("get_action".into()))
    }

    async fn list_actions(
        &self,
        _dataset: Option<&DatasetName>,
    ) -> Result<Vec<Versioned<Action>>, MetadataError> {
        Err(MetadataError::Unsupported("list_actions".into()))
    }

    async fn get_stream(&self, name: &str) -> Result<StreamMeta, MetadataError>;

    async fn stream_update(
        &self,
        meta: &StreamMeta,
        expected_version: i64,
    ) -> Result<StreamMeta, MetadataError>;

    async fn create_stream(&self, name: &str) -> Result<StreamMeta, MetadataError>;

    async fn delete_stream(&self, name: &str, expected_version: i64) -> Result<(), MetadataError>;

    async fn list_streams(&self) -> Result<Vec<StreamMeta>, MetadataError>;

    async fn put_segment(
        &self,
        stream_name: &str,
        segment: &Segment,
        expected_version: i64,
    ) -> Result<Versioned<Segment>, MetadataError>;

    async fn list_segments(
        &self,
        stream_name: &str,
    ) -> Result<Vec<Versioned<Segment>>, MetadataError>;

    async fn get_last_segment(
        &self,
        stream_name: &str,
    ) -> Result<Option<Versioned<Segment>>, MetadataError>;

    async fn get_segment_for_offset(
        &self,
        stream_name: &str,
        offset: i64,
    ) -> Result<Option<Versioned<Segment>>, MetadataError>;

    async fn stream_get_or_insert(&self, name: &str) -> Result<StreamMeta, MetadataError>;

    async fn stream_new_term(&self, name: &str) -> Result<StreamMeta, MetadataError>;

    async fn register_unit(&self, registration: &UnitRegistration) -> Result<(), MetadataError>;

    async fn unregister_unit(&self, address: &str, zone: &str) -> Result<(), MetadataError>;

    async fn list_units(&self) -> Result<Vec<UnitRegistration>, MetadataError>;

    async fn list_writable_units(&self) -> Result<Vec<UnitRegistration>, MetadataError>;

    async fn subscribe_segments(&self, stream_name: &str)
    -> Result<Receiver<String>, MetadataError>;
}

pub type MetadataRef = Arc<dyn Metadata>;

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
pub struct MetadataOptions {
    #[serde(default = "default_service_address")]
    pub service_address: String,
    #[serde(default = "default_namespace")]
    pub namespace: String,
}

impl Default for MetadataOptions {
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

pub async fn build_metadata(options: &MetadataOptions) -> Result<MetadataRef, MetadataError> {
    Ok(Arc::new(build_oxia_metadata(options).await?))
}

pub async fn build_oxia_metadata(options: &MetadataOptions) -> Result<OxiaMetadata, MetadataError> {
    info!(
        address = %options.service_address,
        namespace = %options.namespace,
        "connecting to oxia metadata"
    );
    OxiaMetadata::new(options.service_address.clone(), options.namespace.clone()).await
}
