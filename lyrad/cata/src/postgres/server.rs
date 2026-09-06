use crate::Result;
use crate::options::PostgresOptions;
use crate::postgres::handler::PostgresHandler;
use datafusion::prelude::SessionContext;
use datafusion_postgres::{ServerOptions, serve_with_handlers};
use meta::metadata::Metadata;
use std::sync::Arc;

pub struct PostgresServer {
    // Immutable state
    context: Arc<SessionContext>,
    metadata: Arc<dyn Metadata>,
    options: PostgresOptions,
}

impl PostgresServer {
    pub fn new(
        context: Arc<SessionContext>,
        metadata: Arc<dyn Metadata>,
        options: PostgresOptions,
    ) -> Self {
        Self {
            context,
            metadata,
            options,
        }
    }

    pub async fn serve(&self) -> Result<()> {
        let options = ServerOptions::new()
            .with_host(self.options.host().to_string())
            .with_port(self.options.port())
            .with_max_connections(self.options.max_connections());

        let handler = Arc::new(PostgresHandler::new(
            Arc::clone(&self.context),
            Arc::clone(&self.metadata),
        ));
        serve_with_handlers(handler, &options).await?;
        Ok(())
    }
}
