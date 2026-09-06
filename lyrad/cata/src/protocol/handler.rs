use super::SessionHandler;
use datafusion::prelude::SessionContext;
use datafusion_postgres::pgwire::api::PgWireServerHandlers;
use datafusion_postgres::pgwire::api::auth::StartupHandler;
use datafusion_postgres::pgwire::api::auth::noop::NoopStartupHandler;
use datafusion_postgres::pgwire::api::cancel::{CancelHandler, DefaultCancelHandler};
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::{ConnectionManager, ErrorHandler, NoopHandler};
use meta::metadata::Metadata;
use std::sync::Arc;

pub struct ProtocolHandler {
    // Control state
    cancel: Arc<DefaultCancelHandler>,

    // Immutable state
    session: Arc<SessionHandler>,
    startup: Arc<PostgresStartupHandler>,
}

impl ProtocolHandler {
    pub fn new(context: Arc<SessionContext>, metadata: Arc<dyn Metadata>) -> Self {
        let connection_manager = Arc::new(ConnectionManager::new());
        Self {
            cancel: Arc::new(DefaultCancelHandler::new(Arc::clone(&connection_manager))),
            session: Arc::new(SessionHandler::new(context, metadata)),
            startup: Arc::new(PostgresStartupHandler::new(connection_manager)),
        }
    }
}

impl PgWireServerHandlers for ProtocolHandler {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.session)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.session)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::clone(&self.startup)
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(NoopHandler)
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::clone(&self.cancel)
    }
}

pub struct PostgresStartupHandler {
    // Immutable state
    connection_manager: Arc<ConnectionManager>,
}

impl PostgresStartupHandler {
    pub fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

impl NoopStartupHandler for PostgresStartupHandler {
    fn connection_manager(&self) -> Option<Arc<ConnectionManager>> {
        Some(Arc::clone(&self.connection_manager))
    }
}
