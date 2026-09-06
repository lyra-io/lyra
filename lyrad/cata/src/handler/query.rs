use super::DatabaseHandles;
use crate::error::to_pgwire_error;
use crate::executor::SecretExecutor;
use crate::sql::{CataQueryParser, CatalogStatement, SecretStatement};
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::error::PgWireResult;
use meta::metadata::{DEFAULT_SCHEMA_NAME, Metadata};
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
        statement: CatalogStatement,
    ) -> PgWireResult<Response> {
        match statement {
            CatalogStatement::Secret(SecretStatement::Create(statement)) => {
                self.secret_executor
                    .create(database, DEFAULT_SCHEMA_NAME, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("CREATE SECRET")))
            }
        }
    }
}
