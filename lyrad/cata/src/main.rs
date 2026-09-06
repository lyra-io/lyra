use cata::command::CataCommand;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "lyra-cata", about = "Lyra query and cluster control plane")]
struct Args {
    #[command(flatten)]
    command: CataCommand,
}

#[tokio::main]
async fn main() -> cata::Result<()> {
    let args = Args::parse();
    args.command.run().await
}
