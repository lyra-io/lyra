use crate::CataError;
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError};

pub(crate) fn to_pgwire_error(error: CataError) -> PgWireError {
    let (severity, code) = match error {
        CataError::Sql(_) => ("ERROR", "42601"),
        CataError::UserAlreadyExists(_) => ("ERROR", "42710"),
        CataError::UserNotFound(_) => ("ERROR", "42704"),
        CataError::UserChanged(_) => ("ERROR", "40001"),
        CataError::UserInUse(_) => ("ERROR", "55006"),
        CataError::AuthenticationRequired => ("ERROR", "28000"),
        CataError::SecretAlreadyExists(_) => ("ERROR", "42710"),
        CataError::SecretNotFound(_) => ("ERROR", "42704"),
        CataError::SecretChanged(_) => ("ERROR", "40001"),
        CataError::SecretInUse { .. } => ("ERROR", "2BP01"),
        CataError::DatabaseNotFound(_) => ("ERROR", "3D000"),
        CataError::DatabaseAlreadyExists(_) => ("ERROR", "42P04"),
        CataError::DatabaseChanged(_) => ("ERROR", "40001"),
        CataError::DatabaseInUse(_) => ("ERROR", "55006"),
        CataError::DatabaseNotEmpty(_) => ("ERROR", "2BP01"),
        CataError::SchemaNotFound { .. } => ("ERROR", "3F000"),
        CataError::SchemaAlreadyExists { .. } => ("ERROR", "42P06"),
        CataError::SchemaChanged { .. } => ("ERROR", "40001"),
        CataError::SchemaNotEmpty { .. } => ("ERROR", "2BP01"),
        _ => ("ERROR", "XX000"),
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        severity.to_string(),
        code.to_string(),
        error.to_string(),
    )))
}
