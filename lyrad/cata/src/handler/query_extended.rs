use super::QueryHandler;
use super::{client_database, client_schemas, client_user};
use crate::error::to_pgwire_error;
use crate::sql::{CataQueryParser, DataFusionStatement, parse_catalog_statement};
use async_trait::async_trait;
use datafusion_postgres::pgwire::api::portal::Portal;
use datafusion_postgres::pgwire::api::query::ExtendedQueryHandler;
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::{ClientInfo, ClientPortalStore};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use futures_util::Sink;
use std::fmt::Debug;
use std::sync::Arc;

#[async_trait]
impl ExtendedQueryHandler for QueryHandler {
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
        let database = client_database(client).to_string();
        let schemas = client_schemas(client);
        let current_user = client_user(client).map(ToString::to_string);
        let sql = &portal.statement.statement.0;
        if let Some(statement) = parse_catalog_statement(sql).map_err(to_pgwire_error)? {
            return self
                .execute(
                    &database,
                    &schemas,
                    current_user.as_deref(),
                    statement,
                    Some(&portal.result_column_format),
                )
                .await;
        }

        let datafusion = self.databases.get(&database).map_err(to_pgwire_error)?;
        ExtendedQueryHandler::do_query(datafusion.as_ref(), client, portal, max_rows).await
    }
}
