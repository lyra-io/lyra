use serde::Deserialize;
use std::net::SocketAddr;

use super::auto_config::AutoConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum IoMode {
    Basic,
    #[default]
    Advanced,
    Mmap,
}

#[derive(Debug, Deserialize)]
pub struct StorageOptions {
    #[serde(default = "default_storage_dir")]
    pub dir: String,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            dir: default_storage_dir(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct WalOptions {
    #[serde(default = "default_wal_dir")]
    pub dir: String,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            dir: default_wal_dir(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerOptions {
    #[serde(default = "default_server_address")]
    pub bind_address: SocketAddr,
    #[serde(default)]
    pub advertise_address: Option<String>,
    #[serde(default = "default_zone")]
    pub zone: String,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            bind_address: default_server_address(),
            advertise_address: None,
            zone: default_zone(),
        }
    }
}

fn default_zone() -> String {
    "default".to_string()
}

#[derive(Debug, Deserialize)]
pub struct LogOptions {
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LogOptions {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct CompactionOptions {
    pub interval_ms: Option<u64>,
    pub write_cache_capacity_mb: Option<usize>,
    pub l1_compaction_trigger: Option<usize>,
    pub l2_compaction_trigger: Option<usize>,
    #[serde(default)]
    pub offload: Option<OffloadOptions>,
}

#[derive(Debug, Clone)]
pub struct ResolvedCompactionOptions {
    pub interval_ms: u64,
    pub write_cache_capacity_mb: usize,
    pub l1_compaction_trigger: usize,
    pub l2_compaction_trigger: usize,
    pub offload: Option<OffloadOptions>,
}

impl CompactionOptions {
    pub fn resolve(&self, auto: &AutoConfig) -> ResolvedCompactionOptions {
        ResolvedCompactionOptions {
            interval_ms: self.interval_ms.unwrap_or(auto.compaction_interval_ms),
            write_cache_capacity_mb: self
                .write_cache_capacity_mb
                .unwrap_or(auto.write_cache_capacity_mb),
            l1_compaction_trigger: self
                .l1_compaction_trigger
                .unwrap_or(auto.l1_compaction_trigger),
            l2_compaction_trigger: self
                .l2_compaction_trigger
                .unwrap_or(auto.l2_compaction_trigger),
            offload: self.offload.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct OffloadOptions {
    pub bucket: String,
    #[serde(default = "default_offload_prefix")]
    pub prefix: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct IndexOptions {
    pub block_cache_mb: Option<usize>,
    pub write_buffer_mb: Option<usize>,
    pub num_levels: Option<i32>,
    pub target_file_size_mb: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ResolvedIndexOptions {
    pub block_cache_bytes: usize,
    pub write_buffer_size: usize,
    pub num_levels: i32,
    pub target_file_size_base: u64,
    pub max_bytes_for_level_base: u64,
}

impl IndexOptions {
    pub fn resolve(&self, auto: &AutoConfig) -> ResolvedIndexOptions {
        let block_cache_bytes = self
            .block_cache_mb
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(auto.block_cache_bytes);
        let write_buffer_size = self
            .write_buffer_mb
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(auto.write_buffer_size);
        let target_file_size_base = self
            .target_file_size_mb
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(auto.target_file_size_base);
        let num_levels = self.num_levels.unwrap_or(auto.num_levels);
        let max_bytes_for_level_base = target_file_size_base * 4;

        ResolvedIndexOptions {
            block_cache_bytes,
            write_buffer_size,
            num_levels,
            target_file_size_base,
            max_bytes_for_level_base,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SegmentOptions {
    #[serde(default = "default_segments_dir")]
    pub dir: String,
    pub segment_size_mb: Option<u64>,
}

impl Default for SegmentOptions {
    fn default() -> Self {
        Self {
            dir: default_segments_dir(),
            segment_size_mb: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedSegmentOptions {
    pub dir: String,
    pub segment_size: u64,
}

impl SegmentOptions {
    pub fn resolve(&self, auto: &AutoConfig) -> ResolvedSegmentOptions {
        ResolvedSegmentOptions {
            dir: self.dir.clone(),
            segment_size: self
                .segment_size_mb
                .map(|mb| mb * 1024 * 1024)
                .unwrap_or(auto.segment_size),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct UnitOptions {
    #[serde(default)]
    pub wal: WalOptions,
    #[serde(default)]
    pub storage: StorageOptions,
    #[serde(default)]
    pub server: ServerOptions,
    #[serde(default)]
    pub log: LogOptions,
    #[serde(default)]
    pub compaction: CompactionOptions,
    #[serde(default)]
    pub segments: SegmentOptions,
    #[serde(default)]
    pub io_mode: IoMode,
    #[serde(default)]
    pub index: IndexOptions,
}

fn default_server_address() -> SocketAddr {
    "127.0.0.1:7070".parse().unwrap()
}

fn default_log_level() -> String {
    String::from("info")
}

fn default_storage_dir() -> String {
    let mut path = std::env::temp_dir();
    path.push("chronicle");
    path.push("storage");
    path.to_string_lossy().to_string()
}

fn default_wal_dir() -> String {
    let mut path = std::env::temp_dir();
    path.push("chronicle");
    path.push("wal");
    path.to_string_lossy().to_string()
}

fn default_offload_prefix() -> String {
    "chronicle/segments".to_string()
}

fn default_segments_dir() -> String {
    let mut path = std::env::temp_dir();
    path.push("chronicle");
    path.push("segments");
    path.to_string_lossy().to_string()
}
