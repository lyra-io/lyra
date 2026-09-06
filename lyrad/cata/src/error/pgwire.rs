use crate::CataError;
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError};

pub(crate) fn to_pgwire_error(error: CataError) -> PgWireError {
    let code = match error {
        CataError::Sql(_) => "42601",
        CataError::SecretAlreadyExists(_) => "42710",
        _ => "XX000",
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.to_string(),
        error.to_string(),
    )))
}
