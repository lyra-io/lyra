use cata::Cata;
use cata::options::{CataOptions, PostgresOptions};
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
    postgres: PostgresProcessOptions,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PostgresProcessOptions {
    host: String,
    port: u16,
    max_connections: usize,
}

pub async fn run(args: CataArgs) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt().with_target(false).try_init();

    let options = read_options(&args.config)?;
    let PostgresProcessOptions {
        host,
        port,
        max_connections,
    } = options.cata.postgres;
    let postgres = PostgresOptions::new(host.clone(), port).with_max_connections(max_connections);
    let oxia = OxiaOptions::new(options.meta.service_address, options.meta.namespace);
    let metadata = Arc::new(OxiaMetadata::new(&oxia).await?);
    let cata = Cata::new(CataOptions::new(postgres), metadata)?;

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
        assert_eq!(options.cata.postgres.host, "127.0.0.1");
        assert_eq!(options.cata.postgres.port, 5432);
    }
}
