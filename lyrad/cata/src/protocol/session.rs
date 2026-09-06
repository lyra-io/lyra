use super::DataFusionStatement;
use super::parser::{CataQueryParser, to_pgwire_error};
use crate::handler::SecretHandler;
use crate::sql::{CatalogStatement, SecretStatement, parse_catalog_statement};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion_postgres::DfSessionService;
use datafusion_postgres::pgwire::api::portal::Portal;
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::{ClientInfo, ClientPortalStore};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use futures_util::Sink;
use meta::metadata::Metadata;
use std::fmt::Debug;
use std::sync::Arc;

pub struct SessionHandler {
    // Immutable state
    datafusion: Arc<DfSessionService>,
    parser: Arc<CataQueryParser>,
    secrets: SecretHandler,
}

impl SessionHandler {
    pub fn new(context: Arc<SessionContext>, metadata: Arc<dyn Metadata>) -> Self {
        let datafusion = Arc::new(DfSessionService::new(context));
        let parser = Arc::new(CataQueryParser::new(datafusion.query_parser()));
        let secrets = SecretHandler::new(metadata);
        Self {
            datafusion,
            parser,
            secrets,
        }
    }

    async fn execute(&self, statement: CatalogStatement) -> PgWireResult<Response> {
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

#[async_trait]
impl SimpleQueryHandler for SessionHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let Some(statement) = parse_catalog_statement(query).map_err(to_pgwire_error)? {
            return Ok(vec![self.execute(statement).await?]);
        }

        SimpleQueryHandler::do_query(self.datafusion.as_ref(), client, query).await
    }
}

#[async_trait]
impl ExtendedQueryHandler for SessionHandler {
    type Statement = DataFusionStatement;
    type QueryParser = CataQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.parser)
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &portal.statement.statement.0;
        if let Some(statement) = parse_catalog_statement(sql).map_err(to_pgwire_error)? {
            return self.execute(statement).await;
        }

        ExtendedQueryHandler::do_query(self.datafusion.as_ref(), client, portal, max_rows).await
    }
}
