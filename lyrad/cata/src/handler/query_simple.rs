use super::QueryHandler;
use crate::error::to_pgwire_error;
use crate::sql::parse_catalog_statement;
use async_trait::async_trait;
use datafusion_postgres::pgwire::api::query::SimpleQueryHandler;
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::{ClientInfo, ClientPortalStore};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use futures_util::Sink;
use std::fmt::Debug;

#[async_trait]
impl SimpleQueryHandler for QueryHandler {
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
