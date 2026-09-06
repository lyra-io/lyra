use crate::Result;
use crate::config::PostgresOptions;
use datafusion::prelude::SessionContext;
use datafusion_postgres::{ServerOptions, serve};
use std::sync::Arc;

pub struct PostgresServer {
    // Immutable state
    context: Arc<SessionContext>,
    options: PostgresOptions,
}

impl PostgresServer {
    pub fn new(context: Arc<SessionContext>, options: PostgresOptions) -> Self {
        Self { context, options }
    }

    pub async fn serve(&self) -> Result<()> {
        let options = ServerOptions::new()
            .with_host(self.options.host().to_string())
            .with_port(self.options.port())
            .with_max_connections(self.options.max_connections());

        serve(Arc::clone(&self.context), &options).await?;
        Ok(())
    }
}
