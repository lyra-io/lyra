use clap::Parser;
use lyra_cli::sql::SqlArgs;
use lyra_cli::unit::UnitAction;

#[derive(Parser)]
#[command(name = "lyra", about = "Lyra distributed streaming engine CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    Unit {
        #[command(subcommand)]
        action: UnitAction,
    },
    Sql(SqlArgs),
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Unit { action } => lyra_cli::unit::run(action).await?,
        Commands::Sql(args) => lyra_cli::sql::run(args).await?,
    }

    Ok(())
}
