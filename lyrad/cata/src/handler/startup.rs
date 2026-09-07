use crate::authentication::{AuthenticationHandler, AuthenticationProvider};
use datafusion_postgres::pgwire::api::ConnectionManager;
use datafusion_postgres::pgwire::api::auth::DefaultServerParameterProvider;
use datafusion_postgres::pgwire::api::auth::sasl::SASLAuthStartupHandler;
use std::sync::Arc;

pub(crate) struct StartupHandler {
    // Immutable state
    connection_manager: Arc<ConnectionManager>,
    providers: Vec<Arc<dyn AuthenticationProvider>>,
}

impl StartupHandler {
    pub(crate) fn new(
        connection_manager: Arc<ConnectionManager>,
        providers: Vec<Arc<dyn AuthenticationProvider>>,
    ) -> Self {
        Self {
            connection_manager,
            providers,
        }
    }

    pub(crate) fn new_handler(&self) -> AuthenticationHandler {
        let mut parameters = DefaultServerParameterProvider::default();
        parameters.is_superuser = false;
        let mut handler = SASLAuthStartupHandler::new(Arc::new(parameters))
            .with_connection_manager(Arc::clone(&self.connection_manager));
        for provider in &self.providers {
            handler = provider.configure(handler);
        }
        handler
    }
}
