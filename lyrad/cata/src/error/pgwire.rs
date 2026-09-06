use crate::CataError;
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError};

pub(crate) fn to_pgwire_error(error: CataError) -> PgWireError {
    let (severity, code) = match error {
        CataError::Sql(_) => ("ERROR", "42601"),
        CataError::SecretAlreadyExists(_) => ("ERROR", "42710"),
        CataError::SecretNotFound(_) => ("ERROR", "42704"),
        CataError::SecretChanged(_) => ("ERROR", "40001"),
        CataError::SecretInUse { .. } => ("ERROR", "2BP01"),
        CataError::DatabaseNotFound(_) => ("FATAL", "3D000"),
        CataError::SchemaNotFound { .. } => ("ERROR", "3F000"),
        _ => ("ERROR", "XX000"),
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        severity.to_string(),
        code.to_string(),
        error.to_string(),
    )))
}
