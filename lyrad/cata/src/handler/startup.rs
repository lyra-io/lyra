use super::DatabaseHandles;
use crate::CataError;
use crate::error::to_pgwire_error;
use async_trait::async_trait;
use datafusion_postgres::pgwire::api::ConnectionManager;
use datafusion_postgres::pgwire::api::auth::noop::NoopStartupHandler;
use datafusion_postgres::pgwire::api::{ClientInfo, METADATA_DATABASE};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use futures_util::Sink;
use meta::metadata::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, Metadata};
use std::fmt::Debug;
use std::sync::Arc;

pub(crate) struct StartupHandler {
    // Immutable state
    connection_manager: Arc<ConnectionManager>,
    metadata: Arc<dyn Metadata>,
    databases: Arc<DatabaseHandles>,
}

impl StartupHandler {
    pub(crate) fn new(
        connection_manager: Arc<ConnectionManager>,
        metadata: Arc<dyn Metadata>,
        databases: Arc<DatabaseHandles>,
    ) -> Self {
        Self {
            connection_manager,
            metadata,
            databases,
        }
    }
}

#[async_trait]
impl NoopStartupHandler for StartupHandler {
    fn connection_manager(&self) -> Option<Arc<ConnectionManager>> {
        Some(Arc::clone(&self.connection_manager))
    }

    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let database = client
            .metadata()
            .get(METADATA_DATABASE)
            .cloned()
            .unwrap_or_else(|| DEFAULT_DATABASE_NAME.to_string());
        client
            .metadata_mut()
            .insert(METADATA_DATABASE.to_string(), database.clone());

        if self
            .metadata
            .get_database(&database)
            .await
            .map_err(CataError::from)
            .map_err(to_pgwire_error)?
            .is_none()
        {
            return Err(to_pgwire_error(CataError::DatabaseNotFound(database)));
        }
        if self
            .metadata
            .get_schema(&database, DEFAULT_SCHEMA_NAME)
            .await
            .map_err(CataError::from)
            .map_err(to_pgwire_error)?
            .is_none()
        {
            return Err(to_pgwire_error(CataError::SchemaNotFound {
                database,
                schema: DEFAULT_SCHEMA_NAME.to_string(),
            }));
        }

        self.databases.get(&database).map_err(to_pgwire_error)?;
        Ok(())
    }
}
