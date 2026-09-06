use crate::error::to_pgwire_error;
use crate::handler::SecretHandler;
use crate::sql::{CataQueryParser, CatalogStatement, SecretStatement};
use datafusion::prelude::SessionContext;
use datafusion_postgres::DfSessionService;
use datafusion_postgres::pgwire::api::query::ExtendedQueryHandler;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::error::PgWireResult;
use meta::metadata::Metadata;
use std::sync::Arc;

pub(crate) struct Session {
    // Immutable state
    pub(super) datafusion: Arc<DfSessionService>,
    pub(super) parser: Arc<CataQueryParser>,
    secrets: SecretHandler,
}

impl Session {
    pub(crate) fn new(context: Arc<SessionContext>, metadata: Arc<dyn Metadata>) -> Self {
        let datafusion = Arc::new(DfSessionService::new(context));
        let parser = Arc::new(CataQueryParser::new(datafusion.query_parser()));
        let secrets = SecretHandler::new(metadata);
        Self {
            datafusion,
            parser,
            secrets,
        }
    }

    pub(super) async fn execute(&self, statement: CatalogStatement) -> PgWireResult<Response> {
        match statement {
            CatalogStatement::Secret(SecretStatement::Create(statement)) => {
                self.secrets
                    .create(&statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("CREATE SECRET")))
            }
        }
    }
}
