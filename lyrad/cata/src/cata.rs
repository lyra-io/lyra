use crate::Result;
use crate::handler::{DatabaseHandles, QueryHandler, StartupHandler};
use crate::options::CataOptions;
use datafusion_postgres::pgwire::api::ConnectionManager;
use datafusion_postgres::pgwire::api::PgWireServerHandlers;
use datafusion_postgres::pgwire::api::auth::StartupHandler as PgWireStartupHandler;
use datafusion_postgres::pgwire::api::cancel::{CancelHandler, DefaultCancelHandler};
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::{ServerOptions, serve_with_handlers};
use meta::metadata::{
    DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, Metadata, MetadataError, MetadataPutCondition,
};
use meta::proto::pb_catalog::{Database, Schema};
use std::sync::Arc;

pub struct Cata {
    // Control state
    cancel_handler: Arc<DefaultCancelHandler>,

    // Immutable state
    options: CataOptions,
    query_handler: Arc<QueryHandler>,
    startup_handler: Arc<StartupHandler>,
}

impl Cata {
    pub async fn new(options: CataOptions, metadata: Arc<dyn Metadata>) -> Result<Self> {
        match metadata
            .put_database(
                Database {
                    name: DEFAULT_DATABASE_NAME.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
        {
            Ok(_) | Err(MetadataError::Conflict(_)) => {}
            Err(error) => return Err(error.into()),
        }
        match metadata
            .put_schema(
                DEFAULT_DATABASE_NAME,
                Schema {
                    name: DEFAULT_SCHEMA_NAME.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
        {
            Ok(_) | Err(MetadataError::Conflict(_)) => {}
            Err(error) => return Err(error.into()),
        }

        let databases = Arc::new(DatabaseHandles::new()?);
        let connection_manager = Arc::new(ConnectionManager::new());
        let cancel_handler = Arc::new(DefaultCancelHandler::new(Arc::clone(&connection_manager)));
        let query_handler = Arc::new(QueryHandler::new(
            Arc::clone(&databases),
            Arc::clone(&metadata),
        ));
        let startup_handler =
            Arc::new(StartupHandler::new(connection_manager, metadata, databases));
        Ok(Self {
            cancel_handler,
            options,
            query_handler,
            startup_handler,
        })
    }

    pub async fn serve(self) -> Result<()> {
        let options = ServerOptions::new()
            .with_host(self.options.host().to_string())
            .with_port(self.options.port())
            .with_max_connections(self.options.max_connections());

        serve_with_handlers(Arc::new(self), &options).await?;
        Ok(())
    }
}

impl PgWireServerHandlers for Cata {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.query_handler)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.query_handler)
    }

    fn startup_handler(&self) -> Arc<impl PgWireStartupHandler> {
        Arc::clone(&self.startup_handler)
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::clone(&self.cancel_handler)
    }
}
