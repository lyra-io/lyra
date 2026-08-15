use crate::banner;
use crate::options::UnitOptions;
use crate::process;
use std::io::IsTerminal;
use std::path::Path;
use tracing::info;
use tracing_subscriber::EnvFilter;

const DEFAULT_CONFIG_PATH: &str = "/etc/lyra/options/lyra.toml";
const DEFAULT_PID_FILE: &str = "lyra-unit.pid";

/// Placeholder for the unit runtime.
///
/// The original `Unit` (gRPC service over storage) lived in the monolithic
/// `lyrad` crate and was removed when storage split into the `meta`/`stream`/
/// `tiered` crates. It has not been ported yet, so startup fails fast with a
/// clear message instead of silently doing nothing.
pub struct Unit;

impl Unit {
    pub async fn new(_options: UnitOptions) -> Result<Self, Box<dyn std::error::Error>> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "the unit runtime has not been ported yet after the lyrad storage split",
        )
        .into())
    }

    pub async fn stop(self) {
        info!("unit stopped");
    }
}

#[derive(clap::Subcommand)]
pub enum UnitAction {
    Start {
        #[arg(short, long)]
        config: Option<String>,

        #[arg(long, default_value = DEFAULT_PID_FILE)]
        pid_file: String,
    },

    Stop {
        #[arg(long, default_value = DEFAULT_PID_FILE)]
        pid_file: String,
    },
}

pub async fn run(action: UnitAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        UnitAction::Start { config, pid_file } => {
            let options: UnitOptions = match resolve_config_path(config.as_deref()) {
                Some(path) => read_config(&path)?,
                None => UnitOptions::default(),
            };

            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| EnvFilter::new(&options.liblyra.log_level)),
                )
                .with_ansi(std::io::stderr().is_terminal())
                .with_target(false)
                .with_thread_ids(false)
                .with_thread_names(false)
                .compact()
                .init();

            banner::print_banner("Unit");

            process::write_pid_file(&pid_file)?;

            let unit = Unit::new(options).await?;

            process::wait_for_shutdown().await;

            info!("received shutdown signal");
            unit.stop().await;

            process::remove_pid_file(&pid_file);
        }

        UnitAction::Stop { pid_file } => {
            let pid = process::read_pid_file(&pid_file)?;
            process::send_sigterm(pid)?;
            println!("sent stop signal to unit (pid {})", pid);
        }
    }

    Ok(())
}

fn resolve_config_path(path: Option<&str>) -> Option<String> {
    if let Some(path) = path {
        return Some(path.to_string());
    }
    if let Ok(path) = std::env::var("LYRA_UNIT_CONFIG")
        && !path.trim().is_empty()
    {
        return Some(path);
    }
    if let Ok(path) = std::env::var("LYRA_CONFIG")
        && !path.trim().is_empty()
    {
        return Some(path);
    }
    if Path::new(DEFAULT_CONFIG_PATH).exists() {
        return Some(DEFAULT_CONFIG_PATH.to_string());
    }
    None
}

fn read_config(path: &str) -> Result<UnitOptions, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read config file '{}': {}", path, error))?;
    toml::from_str(&contents)
        .map_err(|error| format!("failed to parse config file '{}': {}", path, error).into())
}
