use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum IoMode {
    Basic,
    #[default]
    Advanced,
    Mmap,
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
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            bind_address: default_server_address(),
        }
    }
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
pub struct UnitOptions {
    #[serde(default)]
    pub wal: WalOptions,
    #[serde(default)]
    pub server: ServerOptions,
    #[serde(default)]
    pub log: LogOptions,
    #[serde(default)]
    pub io_mode: IoMode,
}

fn default_server_address() -> SocketAddr {
    "127.0.0.1:7070".parse().unwrap()
}

fn default_log_level() -> String {
    String::from("info")
}

fn default_wal_dir() -> String {
    let mut path = std::env::temp_dir();
    path.push("chronicle");
    path.push("wal");
    path.to_string_lossy().to_string()
}
