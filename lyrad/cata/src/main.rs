use cata::{Cata, CataOptions};
use clap::Parser;
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_target(false).try_init().ok();

    let args = Args::parse();
    let options = CataOptions::new(args.host, args.port).with_max_connections(args.max_connections);
    let cata = Cata::new(options.clone())?;

    info!(
        host = options.host(),
        port = options.port(),
        "starting Cata"
    );
    cata.serve().await?;
    Ok(())
}
