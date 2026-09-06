use crate::Result;
use crate::config::CataOptions;
use crate::planner::SqlPlanner;
use crate::postgres::PostgresServer;
use datafusion::logical_expr::LogicalPlan;
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
        PostgresServer::new(
            Arc::clone(self.planner.context()),
            Arc::clone(&self.metadata),
            self.options.postgres().clone(),
        )
        .serve()
        .await
    }
}
