use crate::{CataOptions, Result};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_pg_catalog::{pg_catalog::context::EmptyContextProvider, setup_pg_catalog};
use datafusion_postgres::{ServerOptions, serve};
use std::sync::Arc;

const CATALOG_NAME: &str = "lyra";
const SCHEMA_NAME: &str = "public";

pub struct Cata {
    // Immutable state
    context: Arc<SessionContext>,
    options: CataOptions,
}

impl Cata {
    pub fn new(options: CataOptions) -> Result<Self> {
        let config = SessionConfig::new()
            .with_default_catalog_and_schema(CATALOG_NAME, SCHEMA_NAME)
            .with_information_schema(true);
        let context = Arc::new(SessionContext::new_with_config(config));
        setup_pg_catalog(context.as_ref(), CATALOG_NAME, EmptyContextProvider)?;

        Ok(Self { context, options })
    }

    pub fn context(&self) -> &Arc<SessionContext> {
        &self.context
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn initializes_postgresql_catalog() {
        let cata = Cata::new(CataOptions::default()).unwrap();
        let batches = cata
            .context()
            .sql("SELECT current_database(), current_schema()")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
    }
}
