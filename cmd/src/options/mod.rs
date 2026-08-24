//! Unit configuration, split by component.
//!
//! The config file (`options/lyra.toml`) mirrors the crate split: [`stream`]
//! owns the local WAL and segments, and [`LiblyraOptions`] covers the
//! client-facing unit settings. Every component carries a [`MetaOptions`]
//! section for its metadata-service connection.

pub mod stream;

use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
pub struct UnitOptions {
    #[serde(default)]
    pub liblyra: LiblyraOptions,
    #[serde(default)]
    pub stream: stream::StreamOptions,
}

#[derive(Debug, Deserialize)]
pub struct LiblyraOptions {
    #[serde(default = "default_server")]
    pub server: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub meta: MetaOptions,
}

impl Default for LiblyraOptions {
    fn default() -> Self {
        Self {
            server: default_server(),
            log_level: default_log_level(),
            meta: MetaOptions::default(),
        }
    }
}

/// Connection settings for the metadata service.
#[derive(Debug, Deserialize)]
pub struct MetaOptions {
    #[serde(default = "default_service_address")]
    pub service_address: String,
    #[serde(default = "default_namespace")]
    pub namespace: String,
}

impl Default for MetaOptions {
    fn default() -> Self {
        Self {
            service_address: default_service_address(),
            namespace: default_namespace(),
        }
    }
}

fn default_server() -> String {
    "127.0.0.1:7070".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_service_address() -> String {
    "localhost:6648".to_string()
}

fn default_namespace() -> String {
    "default".to_string()
}
