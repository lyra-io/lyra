use chronicle_cli::module::{ModuleAction, ModuleKind};
use chronicle_cli::unit::UnitAction;
use chronicle_cli::verify::VerifyArgs;
use clap::Parser;

#[derive(Parser)]
#[command(name = "chronicle", about = "Chronicle event streaming CLI")]
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
    Catalog {
        #[command(subcommand)]
        action: ModuleAction,
    },
    Sink {
        #[command(subcommand)]
        action: ModuleAction,
    },
    Xunit {
        #[command(subcommand)]
        action: ModuleAction,
    },
    Lens {
        #[command(subcommand)]
        action: ModuleAction,
    },
    Verify(VerifyArgs),
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Unit { action } => chronicle_cli::unit::run(action).await?,
        Commands::Catalog { action } => {
            chronicle_cli::module::run(ModuleKind::Catalog, action).await?
        }
        Commands::Sink { action } => chronicle_cli::module::run(ModuleKind::Sink, action).await?,
        Commands::Xunit { action } => chronicle_cli::module::run(ModuleKind::Xunit, action).await?,
        Commands::Lens { action } => chronicle_cli::module::run(ModuleKind::Lens, action).await?,
        Commands::Verify(args) => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
                )
                .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
                .with_target(false)
                .with_thread_names(false)
                .compact()
                .init();
            chronicle_cli::verify::run(args).await?;
        }
    }

    Ok(())
}
