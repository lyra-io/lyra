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

    #[error("secret {0:?} already exists")]
    SecretAlreadyExists(String),

    #[error("database {0:?} does not exist")]
    DatabaseNotFound(String),

    #[error("schema {schema:?} does not exist in database {database:?}")]
    SchemaNotFound { database: String, schema: String },

    #[error("PostgreSQL server failed: {0}")]
    Server(#[from] io::Error),
}
