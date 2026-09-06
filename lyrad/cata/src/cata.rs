use crate::Result;
use crate::options::CataOptions;
use crate::protocol::ProtocolHandler;
use crate::sql::SqlPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion_postgres::{ServerOptions, serve_with_handlers};
use meta::metadata::Metadata;
use std::sync::Arc;

pub struct Cata {
    // Immutable state
    metadata: Arc<dyn Metadata>,
    planner: SqlPlanner,
    options: CataOptions,
}

impl Cata {
    pub fn new(options: CataOptions, metadata: Arc<dyn Metadata>) -> Result<Self> {
        let planner = SqlPlanner::new()?;
        Ok(Self {
            metadata,
            planner,
            options,
        })
    }

    pub fn metadata(&self) -> &Arc<dyn Metadata> {
        &self.metadata
    }

    pub async fn plan(&self, sql: &str) -> Result<LogicalPlan> {
        self.planner.plan(sql).await
    }

    pub async fn serve(&self) -> Result<()> {
        let postgres = self.options.postgres();
        let options = ServerOptions::new()
            .with_host(postgres.host().to_string())
            .with_port(postgres.port())
            .with_max_connections(postgres.max_connections());
        let handler = Arc::new(ProtocolHandler::new(
            Arc::clone(self.planner.context()),
            Arc::clone(&self.metadata),
        ));

        serve_with_handlers(handler, &options).await?;
        Ok(())
    }
}
