use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::parser::ParserError;
use meta::metadata::MetadataError;
use std::io;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, CataError>;

#[derive(Debug, Error)]
pub enum CataError {
    #[error("invalid SQL: {0}")]
    Sql(#[from] ParserError),

    #[error("failed to plan SQL: {0}")]
    Plan(#[from] DataFusionError),

    #[error("failed to configure the PostgreSQL catalog: {0}")]
    QueryCatalog(#[from] Box<DataFusionError>),

    #[error(transparent)]
    Metadata(#[from] MetadataError),

    #[error("user {0:?} already exists")]
    UserAlreadyExists(String),

    #[error("user {0:?} does not exist")]
    UserNotFound(String),

    #[error("user {0:?} changed concurrently")]
    UserChanged(String),

    #[error("user {0:?} is the current user")]
    UserInUse(String),

    #[error("authentication is required")]
    AuthenticationRequired,

    #[error("permission denied for {0}")]
    PermissionDenied(String),

    #[error("a bootstrap administrator is required when the user catalog is empty")]
    BootstrapAdministratorRequired,

    #[error("the user catalog must contain at least one superuser")]
    AdministratorRequired,

    #[error("user {0:?} has an invalid password credential")]
    InvalidPasswordCredential(String),

    #[error("secret {0:?} already exists")]
    SecretAlreadyExists(String),

    #[error("secret {0:?} does not exist")]
    SecretNotFound(String),

    #[error("secret {0:?} changed concurrently")]
    SecretChanged(String),

    #[error("cannot drop secret {secret:?} because connection {connection:?} depends on it")]
    SecretInUse { secret: String, connection: String },

    #[error("database {0:?} does not exist")]
    DatabaseNotFound(String),

    #[error("database {0:?} already exists")]
    DatabaseAlreadyExists(String),

    #[error("database {0:?} changed concurrently")]
    DatabaseChanged(String),

    #[error("database {0:?} is the current or default database")]
    DatabaseInUse(String),

    #[error("database {0:?} is not empty")]
    DatabaseNotEmpty(String),

    #[error("schema {schema:?} does not exist in database {database:?}")]
    SchemaNotFound { database: String, schema: String },

    #[error("schema {schema:?} already exists in database {database:?}")]
    SchemaAlreadyExists { database: String, schema: String },

    #[error("schema {schema:?} changed concurrently in database {database:?}")]
    SchemaChanged { database: String, schema: String },

    #[error("schema {schema:?} is not empty in database {database:?}")]
    SchemaNotEmpty { database: String, schema: String },

    #[error("PostgreSQL server failed: {0}")]
    Server(#[from] io::Error),
}
