//! Options for the `stream` storage module (local WAL and segments).

use super::MetaOptions;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct StreamOptions {
    #[serde(default = "default_wal_dir")]
    pub wal_dir: PathBuf,
    #[serde(default = "default_io_mode")]
    pub io_mode: String,
    #[serde(default = "default_segments_dir")]
    pub segments_dir: PathBuf,
    #[serde(default)]
    pub meta: MetaOptions,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            wal_dir: default_wal_dir(),
            io_mode: default_io_mode(),
            segments_dir: default_segments_dir(),
            meta: MetaOptions::default(),
        }
    }
}

fn default_wal_dir() -> PathBuf {
    PathBuf::from("/tmp/lyra-wal")
}

fn default_io_mode() -> String {
    "standard".to_string()
}

fn default_segments_dir() -> PathBuf {
    PathBuf::from("/tmp/lyra-segments")
}
