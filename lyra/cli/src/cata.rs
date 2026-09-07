use cata::Cata;
use cata::options::CataOptions;
use clap::Args;
use meta::metadata::oxia::{OxiaMetadata, OxiaOptions};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Args)]
pub struct CataArgs {
    /// Path to the lyrad configuration file.
    #[arg(short, long, value_name = "FILE")]
    pub config: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LyradOptions {
    meta: MetaOptions,
    cata: CataProcessOptions,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetaOptions {
    service_address: String,
    namespace: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CataProcessOptions {
    host: String,
    port: u16,
    max_connections: usize,
    bootstrap_user: Option<String>,
    bootstrap_password: Option<String>,
}

pub async fn run(args: CataArgs) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt().with_target(false).try_init();

    let options = read_options(&args.config)?;
    let CataProcessOptions {
        host,
        port,
        max_connections,
        bootstrap_user,
        bootstrap_password,
    } = options.cata;
    let mut cata_options =
        CataOptions::new(host.clone(), port).with_max_connections(max_connections);
    match (bootstrap_user, bootstrap_password) {
        (Some(user), Some(password)) => {
            cata_options = cata_options.with_bootstrap_user(user, password);
        }
        (None, None) => {}
        _ => {
            return Err(
                "cata.bootstrap_user and cata.bootstrap_password must be configured together"
                    .into(),
            );
        }
    }
    let oxia = OxiaOptions::new(options.meta.service_address, options.meta.namespace);
    let metadata = Arc::new(OxiaMetadata::new(&oxia).await?);
    let cata = Cata::new(cata_options, metadata).await?;

    info!(
        config = %args.config.display(),
        host,
        port,
        max_connections,
        oxia_service_address = oxia.service_address(),
        oxia_namespace = oxia.namespace(),
        "starting Cata"
    );
    cata.serve().await?;
    Ok(())
}

fn read_options(path: &Path) -> Result<LyradOptions, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read config file {:?}: {error}", path))?;
    toml::from_str(&contents)
        .map_err(|error| format!("failed to parse config file {:?}: {error}", path).into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_lyrad_options_file() {
        let options: LyradOptions = toml::from_str(include_str!("../../../options/lyrad.toml"))
            .expect("lyrad options should parse");

        assert_eq!(options.meta.namespace, "default");
        assert_eq!(options.cata.host, "127.0.0.1");
        assert_eq!(options.cata.port, 5432);
    }

    #[test]
    fn allows_bootstrap_credentials_to_be_omitted() {
        let options: LyradOptions = toml::from_str(
            r#"
                [meta]
                service_address = "127.0.0.1:6648"
                namespace = "default"

                [cata]
                host = "127.0.0.1"
                port = 5432
                max_connections = 0
            "#,
        )
        .unwrap();

        assert!(options.cata.bootstrap_user.is_none());
        assert!(options.cata.bootstrap_password.is_none());
    }
}
