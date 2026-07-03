use crate::process;

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
        ModuleAction::Start { config, .. } => {
            let config_note = config
                .as_deref()
                .map(|path| format!(" with config '{}'", path))
                .unwrap_or_default();
            Err(format!(
                "{} module start{} is not implemented yet",
                kind.display_name(),
                config_note
            )
            .into())
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
