use crate::Result;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_pg_catalog::{pg_catalog::context::EmptyContextProvider, setup_pg_catalog};
use std::sync::Arc;

const SCHEMA_NAME: &str = "public";

pub struct SqlPlanner {
    // Immutable state
    context: Arc<SessionContext>,
}

impl SqlPlanner {
    pub fn new(database: &str) -> Result<Self> {
        let config = SessionConfig::new()
            .with_default_catalog_and_schema(database, SCHEMA_NAME)
            .with_information_schema(true);
        let context = Arc::new(SessionContext::new_with_config(config));
        setup_pg_catalog(context.as_ref(), database, EmptyContextProvider)?;

        Ok(Self { context })
    }

    pub(crate) fn context(&self) -> &Arc<SessionContext> {
        &self.context
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;

    #[tokio::test]
    async fn configures_the_current_database() {
        let planner = SqlPlanner::new("analytics").unwrap();
        let batches = planner
            .context()
            .sql("SELECT current_database()")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let database = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(database.value(0), "analytics");
    }
}
