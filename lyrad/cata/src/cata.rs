use crate::Result;
use crate::handler::{DatabaseHandles, QueryHandler, StartupHandler};
use crate::options::CataOptions;
use datafusion_postgres::pgwire::api::ConnectionManager;
use datafusion_postgres::pgwire::api::PgWireServerHandlers;
use datafusion_postgres::pgwire::api::auth::StartupHandler as PgWireStartupHandler;
use datafusion_postgres::pgwire::api::cancel::{CancelHandler, DefaultCancelHandler};
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::{ServerOptions, serve_with_handlers};
use meta::auth::{BasicAuthenticationProvider, make_password_credential};
use meta::metadata::{
    DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, Metadata, MetadataError, MetadataPutCondition,
};
use meta::proto::pb_catalog::{Database, Schema, User};
use std::sync::Arc;

pub struct Cata {
    // Control state
    cancel_handler: Arc<DefaultCancelHandler>,

    // Immutable state
    options: CataOptions,
    query_handler: Arc<QueryHandler>,
    startup_handler: Arc<StartupHandler>,
}

impl Cata {
    pub async fn new(options: CataOptions, metadata: Arc<dyn Metadata>) -> Result<Self> {
        Self::bootstrap_user0(&options, &metadata).await?;
        Self::bootstrap_database0(&metadata).await?;

        let databases = Arc::new(DatabaseHandles::new(Arc::clone(&metadata))?);
        let connection_manager = Arc::new(ConnectionManager::new());
        let cancel_handler = Arc::new(DefaultCancelHandler::new(Arc::clone(&connection_manager)));
        let query_handler = Arc::new(QueryHandler::new(
            Arc::clone(&databases),
            Arc::clone(&metadata),
        ));
        let authentication_provider =
            Arc::new(BasicAuthenticationProvider::new(Arc::clone(&metadata)));
        let startup_handler = Arc::new(StartupHandler::new(
            connection_manager,
            authentication_provider,
            metadata,
            Arc::clone(&databases),
        ));
        Ok(Self {
            cancel_handler,
            options,
            query_handler,
            startup_handler,
        })
    }

    async fn bootstrap_user0(options: &CataOptions, metadata: &Arc<dyn Metadata>) -> Result<()> {
        if metadata.list_users().await?.is_empty() {
            let (name, password) = options
                .bootstrap_user()
                .ok_or(crate::CataError::BootstrapUserRequired)?;
            let user = User {
                name: name.to_string(),
                password: Some(make_password_credential(password)),
                id: metadata.allocate_user_id().await?,
            };
            match metadata
                .put_user(user, MetadataPutCondition::NotExists)
                .await
            {
                Ok(_) | Err(MetadataError::Conflict(_)) => {}
                Err(error) => return Err(error.into()),
            }
        }

        if metadata.list_users().await?.is_empty() {
            Err(crate::CataError::BootstrapUserRequired)
        } else {
            Ok(())
        }
    }

    async fn bootstrap_database0(metadata: &Arc<dyn Metadata>) -> Result<()> {
        match metadata.get_database(DEFAULT_DATABASE_NAME).await? {
            Some(_) => {}
            None => {
                match metadata
                    .put_database(
                        Database {
                            name: DEFAULT_DATABASE_NAME.to_string(),
                        },
                        MetadataPutCondition::NotExists,
                    )
                    .await
                {
                    Ok(_) | Err(MetadataError::Conflict(_)) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        match metadata
            .get_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
            .await?
        {
            Some(_) => {}
            None => {
                match metadata
                    .put_schema(
                        DEFAULT_DATABASE_NAME,
                        Schema {
                            name: DEFAULT_SCHEMA_NAME.to_string(),
                        },
                        MetadataPutCondition::NotExists,
                    )
                    .await
                {
                    Ok(_) | Err(MetadataError::Conflict(_)) => {}
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Ok(())
    }

    pub async fn serve(self) -> Result<()> {
        let options = ServerOptions::new()
            .with_host(self.options.host().to_string())
            .with_port(self.options.port())
            .with_max_connections(self.options.max_connections());

        serve_with_handlers(Arc::new(self), &options).await?;
        Ok(())
    }
}

impl PgWireServerHandlers for Cata {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.query_handler)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.query_handler)
    }

    fn startup_handler(&self) -> Arc<impl PgWireStartupHandler> {
        Arc::new(self.startup_handler.new_handler())
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::clone(&self.cancel_handler)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meta::metadata::MemoryMetadata;

    #[tokio::test]
    async fn bootstraps_the_user_and_default_catalog() {
        let metadata = Arc::new(MemoryMetadata::new());
        let options = CataOptions::default().with_bootstrap_user("root", "s3cr3t");

        let _cata = Cata::new(options, metadata.clone()).await.unwrap();

        let user = metadata.get_user("root").await.unwrap().unwrap();
        assert!(user.value().password.is_some());
        assert!(
            metadata
                .get_database(DEFAULT_DATABASE_NAME)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            metadata
                .get_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn requires_a_bootstrap_user_for_an_empty_catalog() {
        let metadata = Arc::new(MemoryMetadata::new());

        let result = Cata::new(CataOptions::default(), metadata).await;

        assert!(matches!(
            result,
            Err(crate::CataError::BootstrapUserRequired)
        ));
    }
}
