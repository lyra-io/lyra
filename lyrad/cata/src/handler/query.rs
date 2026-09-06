use super::DatabaseHandles;
use crate::error::to_pgwire_error;
use crate::executor::SecretExecutor;
use crate::sql::{CataQueryParser, CatalogStatement, SecretStatement, show_secrets_fields};
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use datafusion_postgres::pgwire::error::PgWireResult;
use futures_util::stream;
use meta::metadata::Metadata;
use std::sync::Arc;

pub(crate) struct QueryHandler {
    // Immutable state
    pub(super) databases: Arc<DatabaseHandles>,
    pub(super) parser: Arc<CataQueryParser>,
    secret_executor: SecretExecutor,
}

impl QueryHandler {
    pub(crate) fn new(databases: Arc<DatabaseHandles>, metadata: Arc<dyn Metadata>) -> Self {
        let parser = Arc::new(CataQueryParser::new(Arc::clone(&databases)));
        let secret_executor = SecretExecutor::new(metadata);
        Self {
            databases,
            parser,
            secret_executor,
        }
    }

    pub(super) async fn execute(
        &self,
        database: &str,
        schemas: &[String],
        statement: CatalogStatement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Response> {
        match statement {
            CatalogStatement::Secret(SecretStatement::Create(statement)) => {
                self.secret_executor
                    .create(database, schemas, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("CREATE SECRET")))
            }
            CatalogStatement::Secret(SecretStatement::Alter(statement)) => {
                self.secret_executor
                    .alter(database, schemas, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("ALTER SECRET")))
            }
            CatalogStatement::Secret(SecretStatement::Drop(statement)) => {
                self.secret_executor
                    .drop(database, schemas, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("DROP SECRET")))
            }
            CatalogStatement::Secret(SecretStatement::Show(statement)) => {
                let secrets = self
                    .secret_executor
                    .show(database, schemas, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                let fields = Arc::new(show_secrets_fields(column_format));
                let row_fields = Arc::clone(&fields);
                let rows = stream::iter(secrets.into_iter().map(move |name| {
                    let mut encoder = DataRowEncoder::new(Arc::clone(&row_fields));
                    encoder.encode_field(&Some(name.as_str()))?;
                    Ok(encoder.take_row())
                }));
                let mut response = QueryResponse::new(fields, rows);
                response.set_command_tag("SHOW");
                Ok(Response::Query(response))
            }
        }
    }
}
