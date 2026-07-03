use crate::banner;
use crate::process;
use catalog::{CatalogOptions, build_catalog};
use chronicle_sink::{Sink, SinkOptions};
use chronicle_xunit::Xunit;
use serde::Deserialize;
use std::io::IsTerminal;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use tracing::info;
use tracing_subscriber::EnvFilter;

const DEFAULT_CONFIG_PATH: &str = "/etc/chronicle/chronicled.toml";

#[derive(Clone, Copy)]
pub enum ModuleKind {
    Catalog,
    Sink,
    Xunit,
    Lens,
}

impl ModuleKind {
    fn command_name(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Sink => "sink",
            Self::Xunit => "xunit",
            Self::Lens => "lens",
        }
    }

    fn display_name(self) -> &'static str {
        match self {
            Self::Catalog => "Catalog",
            Self::Sink => "Sink",
            Self::Xunit => "XUnit",
            Self::Lens => "Lens",
        }
    }

    fn default_pid_file(self) -> String {
        format!("chronicle-{}.pid", self.command_name())
    }
}

#[derive(clap::Subcommand)]
pub enum ModuleAction {
    Start {
        #[arg(short, long)]
        config: Option<String>,

        #[arg(long)]
        pid_file: Option<String>,
    },

    Stop {
        #[arg(long)]
        pid_file: Option<String>,
    },
}

pub async fn run(kind: ModuleKind, action: ModuleAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        ModuleAction::Start { config, pid_file } => {
            let config = load_config(config.as_deref())?;
            init_tracing(&config.log.level);
            banner::print_banner(kind.display_name());

            let pid_file = pid_file.unwrap_or_else(|| kind.default_pid_file());
            process::write_pid_file(&pid_file)?;

            let catalog = build_catalog(&config.catalog).await?;
            let mut wait_for_shutdown_after_start = true;
            match kind {
                ModuleKind::Catalog => {
                    info!("catalog component connected to Oxia");
                }
                ModuleKind::Sink => {
                    Sink::new(catalog, SinkOptions::default()).start().await?;
                }
                ModuleKind::Xunit => {
                    let _xunit = Xunit::new(catalog);
                    info!("xunit component started");
                }
                ModuleKind::Lens => {
                    wait_for_shutdown_after_start = false;
                    let lens = chronicle_lens::Lens::new(catalog);
                    chronicle_lens::flight_sql::serve_with_shutdown(
                        lens,
                        config.lens.bind_address,
                        process::wait_for_shutdown(),
                    )
                    .await?;
                }
            }

            if wait_for_shutdown_after_start {
                process::wait_for_shutdown().await;
            }
            info!(component = kind.command_name(), "received shutdown signal");
            process::remove_pid_file(&pid_file);
            Ok(())
        }
        ModuleAction::Stop { pid_file } => {
            let pid_file = pid_file.unwrap_or_else(|| kind.default_pid_file());
            let pid = process::read_pid_file(&pid_file)?;
            process::send_sigterm(pid)?;
            println!(
                "sent stop signal to {} module (pid {})",
                kind.command_name(),
                pid
            );
            Ok(())
        }
    }
}

#[derive(Debug, Deserialize)]
struct ModuleConfig {
    #[serde(default)]
    catalog: CatalogOptions,
    #[serde(default)]
    lens: LensConfig,
    #[serde(default)]
    log: LogConfig,
}

#[derive(Debug, Deserialize)]
struct LensConfig {
    #[serde(default = "default_lens_bind_address")]
    bind_address: SocketAddr,
}

impl Default for LensConfig {
    fn default() -> Self {
        Self {
            bind_address: default_lens_bind_address(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct LogConfig {
    #[serde(default = "default_log_level")]
    level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

fn load_config(path: Option<&str>) -> Result<ModuleConfig, Box<dyn std::error::Error>> {
    match resolve_config_path(path) {
        Some(path) => read_config(&path),
        None => Ok(ModuleConfig {
            catalog: CatalogOptions::default(),
            lens: LensConfig::default(),
            log: LogConfig::default(),
        }),
    }
}

fn default_lens_bind_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 50051)
}

fn resolve_config_path(path: Option<&str>) -> Option<String> {
    if let Some(path) = path {
        return Some(path.to_string());
    }
    if let Ok(path) = std::env::var("CHRONICLE_CONFIG")
        && !path.trim().is_empty()
    {
        return Some(path);
    }
    if Path::new(DEFAULT_CONFIG_PATH).exists() {
        return Some(DEFAULT_CONFIG_PATH.to_string());
    }
    None
}

fn read_config(path: &str) -> Result<ModuleConfig, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read config file '{}': {}", path, error))?;
    toml::from_str(&contents)
        .map_err(|error| format!("failed to parse config file '{}': {}", path, error).into())
}

fn init_tracing(level: &str) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level)),
        )
        .with_ansi(std::io::stderr().is_terminal())
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .compact()
        .try_init();
}
