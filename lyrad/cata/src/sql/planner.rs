use crate::Result;
use crate::sql::{CatalogContextProvider, register_rw_catalog};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_pg_catalog::setup_pg_catalog;
use meta::metadata::Metadata;
use std::sync::Arc;

const SCHEMA_NAME: &str = "public";

pub struct SqlPlanner {
    // Immutable state
    context: Arc<SessionContext>,
}

impl SqlPlanner {
    pub fn new(database: &str, metadata: Arc<dyn Metadata>) -> Result<Self> {
        let config = SessionConfig::new()
            .with_default_catalog_and_schema(database, SCHEMA_NAME)
            .with_information_schema(true);
        let context = Arc::new(SessionContext::new_with_config(config));
        setup_pg_catalog(
            context.as_ref(),
            database,
            CatalogContextProvider::new(Arc::clone(&metadata)),
        )?;
        register_rw_catalog(context.as_ref(), database, metadata)?;

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
    use meta::metadata::{MemoryMetadata, MetadataPutCondition};
    use meta::proto::pb_catalog::User;

    #[tokio::test]
    async fn configures_the_current_database() {
        let planner = SqlPlanner::new("analytics", Arc::new(MemoryMetadata::new())).unwrap();
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

    #[tokio::test]
    async fn exposes_catalog_users() {
        let metadata = Arc::new(MemoryMetadata::new());
        metadata
            .put_user(
                User {
                    name: "alice".to_string(),
                    password: None,
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let planner = SqlPlanner::new("analytics", metadata).unwrap();

        let roles = planner
            .context()
            .sql("SELECT rolname FROM pg_catalog.pg_roles")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let names = roles[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");

        let users = planner
            .context()
            .sql("SELECT name FROM rw_catalog.rw_users")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let names = users[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
    }
}
