use arrow_array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, UInt64Array,
};
use arrow_flight::sql::client::FlightSqlServiceClient;
use clap::Args;
use futures_util::TryStreamExt;
use serde::Deserialize;
use std::io::{self, Write};
use std::path::Path;
use tonic::transport::{Channel, Endpoint};
use tracing::info;
use tracing_subscriber::EnvFilter;

const DEFAULT_CONFIG_PATH: &str = "/etc/chronicle/chronicled.toml";

#[derive(Debug, Args)]
pub struct SqlArgs {
    #[arg(short, long)]
    pub config: Option<String>,

    #[arg(long)]
    pub endpoint: Option<String>,

    #[arg(short = 'e', long)]
    pub execute: Option<String>,
}

pub async fn run(args: SqlArgs) -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config(args.config.as_deref())?;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log.level)),
        )
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .compact()
        .try_init();

    let endpoint = args.endpoint.unwrap_or(config.lens.endpoint);
    info!(endpoint = %endpoint, "connecting to lens Flight SQL endpoint");
    let mut client = connect_client(&endpoint).await?;

    if let Some(statement) = args.execute {
        execute_and_print(&mut client, &statement).await?;
        return Ok(());
    }

    repl(&mut client).await
}

async fn repl(
    client: &mut FlightSqlServiceClient<Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut line = String::new();

    loop {
        print!("chronicle> ");
        io::stdout().flush()?;

        line.clear();
        if stdin.read_line(&mut line)? == 0 {
            break;
        }

        let statement = line.trim();
        if statement.is_empty() {
            continue;
        }
        if statement.eq_ignore_ascii_case("quit")
            || statement.eq_ignore_ascii_case("exit")
            || statement.eq_ignore_ascii_case("\\q")
        {
            break;
        }

        if let Err(error) = execute_and_print(client, statement).await {
            eprintln!("error: {error}");
        }
    }

    Ok(())
}

async fn execute_and_print(
    client: &mut FlightSqlServiceClient<Channel>,
    statement: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let flight_info = client.execute(statement.to_string(), None).await?;
    let mut batches = Vec::new();
    for endpoint in flight_info.endpoint {
        let Some(ticket) = endpoint.ticket else {
            continue;
        };
        let flight_data = client.do_get(ticket).await?;
        let mut endpoint_batches: Vec<RecordBatch> = flight_data.try_collect().await?;
        batches.append(&mut endpoint_batches);
    }
    print_batches(&batches);
    Ok(())
}

async fn connect_client(
    endpoint: &str,
) -> Result<FlightSqlServiceClient<Channel>, Box<dyn std::error::Error>> {
    let endpoint = normalize_endpoint(endpoint);
    let channel = Endpoint::from_shared(endpoint)?.connect().await?;
    Ok(FlightSqlServiceClient::new(channel))
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

fn print_batches(batches: &[RecordBatch]) {
    for batch in batches {
        for row in 0..batch.num_rows() {
            let values = batch
                .columns()
                .iter()
                .map(|array| value_to_string(array.as_ref(), row))
                .collect::<Vec<_>>();
            println!("{}", values.join("\t"));
        }
    }
}

fn value_to_string(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "NULL".to_string();
    }

    match array.data_type() {
        arrow_schema::DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|array| array.value(row).to_string())
            .unwrap_or_default(),
        arrow_schema::DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|array| array.value(row).to_string())
            .unwrap_or_default(),
        arrow_schema::DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|array| array.value(row).to_string())
            .unwrap_or_default(),
        arrow_schema::DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|array| array.value(row).to_string())
            .unwrap_or_default(),
        arrow_schema::DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|array| array.value(row).to_string())
            .unwrap_or_default(),
        arrow_schema::DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|array| array.value(row).to_string())
            .unwrap_or_default(),
        arrow_schema::DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|array| array.value(row).to_string())
            .unwrap_or_default(),
        arrow_schema::DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|array| String::from_utf8_lossy(array.value(row)).into_owned())
            .unwrap_or_default(),
        other => format!("<{other}>"),
    }
}

#[derive(Debug, Deserialize)]
struct SqlConfig {
    #[serde(default)]
    lens: LensClientConfig,
    #[serde(default)]
    log: LogConfig,
}

#[derive(Debug, Deserialize)]
struct LensClientConfig {
    #[serde(default = "default_lens_endpoint")]
    endpoint: String,
}

impl Default for LensClientConfig {
    fn default() -> Self {
        Self {
            endpoint: default_lens_endpoint(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct LogConfig {
    #[serde(default = "default_log_level")]
    level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_lens_endpoint() -> String {
    "http://127.0.0.1:50051".to_string()
}

fn load_config(path: Option<&str>) -> Result<SqlConfig, Box<dyn std::error::Error>> {
    match resolve_config_path(path) {
        Some(path) => read_config(&path),
        None => Ok(SqlConfig {
            lens: LensClientConfig::default(),
            log: LogConfig::default(),
        }),
    }
}

fn resolve_config_path(path: Option<&str>) -> Option<String> {
    if let Some(path) = path {
        return Some(path.to_string());
    }
    if let Ok(path) = std::env::var("CHRONICLE_CONFIG")
        && !path.trim().is_empty()
    {
        return Some(path);
    }
    if Path::new(DEFAULT_CONFIG_PATH).exists() {
        return Some(DEFAULT_CONFIG_PATH.to_string());
    }
    None
}

fn read_config(path: &str) -> Result<SqlConfig, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read config file '{}': {}", path, error))?;
    toml::from_str(&contents)
        .map_err(|error| format!("failed to parse config file '{}': {}", path, error).into())
}
