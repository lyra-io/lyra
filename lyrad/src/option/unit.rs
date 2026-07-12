use serde::Deserialize;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Debug, Clone, Deserialize)]
pub struct UnitOptions {
    #[serde(default)]
    pub io_mode: IoMode,
    #[serde(default)]
    pub server: ServerOptions,
    #[serde(default)]
    pub wal: WalOptions,
    #[serde(default)]
    pub storage: StorageOptions,
    #[serde(default)]
    pub segments: SegmentOptions,
    #[serde(default)]
    pub log: LogOptions,
}

impl Default for UnitOptions {
    fn default() -> Self {
        Self {
            io_mode: IoMode::Basic,
            server: ServerOptions::default(),
            wal: WalOptions::default(),
            storage: StorageOptions::default(),
            segments: SegmentOptions::default(),
            log: LogOptions::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IoMode {
    Basic,
    Direct,
    Mmap,
}

impl Default for IoMode {
    fn default() -> Self {
        Self::Basic
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerOptions {
    #[serde(default = "default_bind_address")]
    pub bind_address: SocketAddr,
    #[serde(default)]
    pub advertise_address: Option<String>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            bind_address: default_bind_address(),
            advertise_address: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
pub struct SegmentOptions {
    #[serde(default = "default_segments_dir")]
    pub dir: String,
}

impl Default for SegmentOptions {
    fn default() -> Self {
        Self {
            dir: default_segments_dir(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
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

fn default_bind_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 7070)
}

fn default_wal_dir() -> String {
    "/data/wal".to_string()
}

fn default_storage_dir() -> String {
    "/data/storage".to_string()
}

fn default_segments_dir() -> String {
    "/data/segments".to_string()
}

fn default_log_level() -> String {
    "info".to_string()
}
