use crate::Result;
use crate::options::CataOptions;
use crate::session::Session;
use crate::sql::SqlPlanner;
use datafusion_postgres::pgwire::api::ConnectionManager;
use datafusion_postgres::pgwire::api::PgWireServerHandlers;
use datafusion_postgres::pgwire::api::auth::StartupHandler;
use datafusion_postgres::pgwire::api::auth::noop::NoopStartupHandler;
use datafusion_postgres::pgwire::api::cancel::{CancelHandler, DefaultCancelHandler};
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::{ServerOptions, serve_with_handlers};
use meta::metadata::Metadata;
use std::sync::Arc;

pub struct Cata {
    // Control state
    cancel: Arc<DefaultCancelHandler>,

    // Immutable state
    options: CataOptions,
    session: Arc<Session>,
    startup: Arc<PostgresStartupHandler>,
}

impl Cata {
    pub fn new(options: CataOptions, metadata: Arc<dyn Metadata>) -> Result<Self> {
        let planner = SqlPlanner::new()?;
        let connection_manager = Arc::new(ConnectionManager::new());
        let cancel = Arc::new(DefaultCancelHandler::new(Arc::clone(&connection_manager)));
        let session = Arc::new(Session::new(Arc::clone(planner.context()), metadata));
        let startup = Arc::new(PostgresStartupHandler::new(connection_manager));
        Ok(Self {
            cancel,
            options,
            session,
            startup,
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
        Arc::clone(&self.session)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.session)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::clone(&self.startup)
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::clone(&self.cancel)
    }
}

struct PostgresStartupHandler {
    // Immutable state
    connection_manager: Arc<ConnectionManager>,
}

impl PostgresStartupHandler {
    fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

impl NoopStartupHandler for PostgresStartupHandler {
    fn connection_manager(&self) -> Option<Arc<ConnectionManager>> {
        Some(Arc::clone(&self.connection_manager))
    }
}
