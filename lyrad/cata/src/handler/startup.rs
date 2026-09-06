use datafusion_postgres::pgwire::api::ConnectionManager;
use datafusion_postgres::pgwire::api::auth::noop::NoopStartupHandler;
use std::sync::Arc;

pub(crate) struct StartupHandler {
    // Immutable state
    connection_manager: Arc<ConnectionManager>,
}

impl StartupHandler {
    pub(crate) fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

impl NoopStartupHandler for StartupHandler {
    fn connection_manager(&self) -> Option<Arc<ConnectionManager>> {
        Some(Arc::clone(&self.connection_manager))
    }
}
