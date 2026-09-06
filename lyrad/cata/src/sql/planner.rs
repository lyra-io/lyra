use crate::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_pg_catalog::{pg_catalog::context::EmptyContextProvider, setup_pg_catalog};
use std::sync::Arc;

const CATALOG_NAME: &str = "lyra";
const SCHEMA_NAME: &str = "public";

pub struct SqlPlanner {
    // Immutable state
    context: Arc<SessionContext>,
}

impl SqlPlanner {
    pub fn new() -> Result<Self> {
        let config = SessionConfig::new()
            .with_default_catalog_and_schema(CATALOG_NAME, SCHEMA_NAME)
            .with_information_schema(true);
        let context = Arc::new(SessionContext::new_with_config(config));
        setup_pg_catalog(context.as_ref(), CATALOG_NAME, EmptyContextProvider)?;

        Ok(Self { context })
    }

    pub async fn plan(&self, sql: &str) -> Result<LogicalPlan> {
        Ok(self.context.state().create_logical_plan(sql).await?)
    }

    pub(crate) fn context(&self) -> &Arc<SessionContext> {
        &self.context
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn creates_a_logical_plan_without_executing_it() {
        let planner = SqlPlanner::new().unwrap();
        let plan = planner.plan("SELECT 1 + 2 AS result").await.unwrap();

        assert!(plan.display_indent().to_string().contains("Projection"));
    }
}
