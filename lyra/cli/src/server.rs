use crate::process;
use std::fmt::{Display, Formatter};

const DEFAULT_CONFIG_PATH: &str = "/etc/lyra/options/lyra.toml";

#[derive(Debug, clap::Subcommand)]
pub enum ServerCommand {
    /// Manage the function server.
    Function {
        #[command(subcommand)]
        action: ServerAction,
    },

    /// Manage the table server.
    Table {
        #[command(subcommand)]
        action: ServerAction,
    },

    /// Manage the SQL server.
    Sql {
        #[command(subcommand)]
        action: ServerAction,
    },
}

#[derive(Debug, clap::Subcommand)]
pub enum ServerAction {
    /// Start the server in the foreground.
    Start {
        #[arg(short, long, default_value = DEFAULT_CONFIG_PATH)]
        config: String,

        #[arg(long)]
        pid_file: Option<String>,
    },

    /// Stop a server using its PID file.
    Stop {
        #[arg(long)]
        pid_file: Option<String>,
    },
}

#[derive(Debug, Clone, Copy)]
enum ServerRole {
    Function,
    Table,
    Sql,
}

impl ServerRole {
    fn default_pid_file(self) -> String {
        format!("lyra-{self}.pid")
    }
}

impl Display for ServerRole {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Function => formatter.write_str("function"),
            Self::Table => formatter.write_str("table"),
            Self::Sql => formatter.write_str("sql"),
        }
    }
}

pub async fn run(command: ServerCommand) -> Result<(), Box<dyn std::error::Error>> {
    let (role, action) = match command {
        ServerCommand::Function { action } => (ServerRole::Function, action),
        ServerCommand::Table { action } => (ServerRole::Table, action),
        ServerCommand::Sql { action } => (ServerRole::Sql, action),
    };

    match action {
        ServerAction::Start { config, pid_file } => {
            let _ = (config, pid_file);
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!("the {role} server runtime has not been implemented yet"),
            )
            .into())
        }
        ServerAction::Stop { pid_file } => {
            let pid_file = pid_file.unwrap_or_else(|| role.default_pid_file());
            let pid = process::read_pid_file(&pid_file)?;
            process::send_sigterm(pid)?;
            println!("sent stop signal to {role} server (pid {pid})");
            Ok(())
        }
    }
}
