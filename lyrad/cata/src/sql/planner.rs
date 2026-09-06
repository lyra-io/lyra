use crate::Result;
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

    pub(crate) fn context(&self) -> &Arc<SessionContext> {
        &self.context
    }
}
