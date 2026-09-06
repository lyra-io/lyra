use cata::Cata;
use cata::config::{CataOptions, PostgresOptions};
use clap::Parser;
use meta::metadata::oxia::{OxiaMetadata, OxiaOptions};
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Parser)]
#[command(name = "lyra-cata", about = "Lyra query and cluster control plane")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = 5432)]
    port: u16,

    #[arg(long, default_value_t = 0)]
    max_connections: usize,

    #[arg(long, default_value = "127.0.0.1:6648")]
    oxia_service_address: String,

    #[arg(long, default_value = "default")]
    oxia_namespace: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_target(false).try_init().ok();

    let args = Args::parse();
    let postgres =
        PostgresOptions::new(args.host, args.port).with_max_connections(args.max_connections);
    let oxia = OxiaOptions::new(args.oxia_service_address, args.oxia_namespace);
    let metadata = Arc::new(OxiaMetadata::new(&oxia).await?);
    let options = CataOptions::new(postgres);
    let cata = Cata::new(options.clone(), metadata)?;

    info!(
        host = options.postgres().host(),
        port = options.postgres().port(),
        oxia_service_address = oxia.service_address(),
        oxia_namespace = oxia.namespace(),
        "starting Cata"
    );
    cata.serve().await?;
    Ok(())
}
