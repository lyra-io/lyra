use crate::CataError;
use crate::error::to_pgwire_error;
use crate::handler::DatabaseHandles;
use async_trait::async_trait;
use datafusion_postgres::pgwire::api::ConnectionManager;
use datafusion_postgres::pgwire::api::auth::DefaultServerParameterProvider;
use datafusion_postgres::pgwire::api::auth::sasl::SASLAuthStartupHandler;
use datafusion_postgres::pgwire::api::auth::sasl::scram::{SCRAM_ITERATIONS, ScramAuth};
use datafusion_postgres::pgwire::api::auth::{AuthSource, LoginInfo, Password};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use meta::auth::{AuthenticationError, BASIC_PASSWORD_ITERATIONS, BasicAuthenticationProvider};
use meta::metadata::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, Metadata};
use std::fmt;
use std::sync::Arc;

type AuthenticationHandler = SASLAuthStartupHandler<DefaultServerParameterProvider>;

pub(crate) struct StartupHandler {
    // Immutable state
    connection_manager: Arc<ConnectionManager>,
    source: Arc<MetadataPasswordSource>,
}

impl StartupHandler {
    pub(crate) fn new(
        connection_manager: Arc<ConnectionManager>,
        provider: Arc<BasicAuthenticationProvider>,
        metadata: Arc<dyn Metadata>,
        databases: Arc<DatabaseHandles>,
    ) -> Self {
        Self {
            connection_manager,
            source: Arc::new(MetadataPasswordSource::new(provider, metadata, databases)),
        }
    }

    pub(crate) fn new_handler(&self) -> AuthenticationHandler {
        let mut parameters = DefaultServerParameterProvider::default();
        parameters.is_superuser = false;
        let source: Arc<dyn AuthSource> = self.source.clone();
        let mut scram = ScramAuth::new(source);
        debug_assert_eq!(BASIC_PASSWORD_ITERATIONS as usize, SCRAM_ITERATIONS);
        scram.set_iterations(BASIC_PASSWORD_ITERATIONS as usize);
        SASLAuthStartupHandler::new(Arc::new(parameters))
            .with_connection_manager(Arc::clone(&self.connection_manager))
            .with_scram(scram)
    }
}

struct MetadataPasswordSource {
    // Immutable state
    provider: Arc<BasicAuthenticationProvider>,
    metadata: Arc<dyn Metadata>,
    databases: Arc<DatabaseHandles>,
}

impl MetadataPasswordSource {
    fn new(
        provider: Arc<BasicAuthenticationProvider>,
        metadata: Arc<dyn Metadata>,
        databases: Arc<DatabaseHandles>,
    ) -> Self {
        Self {
            provider,
            metadata,
            databases,
        }
    }

    async fn password0(&self, login: &LoginInfo<'_>) -> PgWireResult<Password> {
        let name = login
            .user()
            .ok_or_else(|| to_pgwire_error(CataError::AuthenticationRequired))?;
        let credential =
            self.provider
                .password_credential(name)
                .await
                .map_err(|error| match error {
                    AuthenticationError::InvalidCredentials => {
                        PgWireError::InvalidPassword(name.to_string())
                    }
                    AuthenticationError::Metadata(error) => {
                        to_pgwire_error(CataError::Metadata(error))
                    }
                })?;

        let database = login.database().unwrap_or(DEFAULT_DATABASE_NAME);
        if self
            .metadata
            .get_database(database)
            .await
            .map_err(CataError::from)
            .map_err(to_pgwire_error)?
            .is_none()
        {
            return Err(to_pgwire_error(CataError::DatabaseNotFound(
                database.to_string(),
            )));
        }
        if self
            .metadata
            .get_schema(database, DEFAULT_SCHEMA_NAME)
            .await
            .map_err(CataError::from)
            .map_err(to_pgwire_error)?
            .is_none()
        {
            return Err(to_pgwire_error(CataError::SchemaNotFound {
                database: database.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
            }));
        }
        self.databases.get(database).map_err(to_pgwire_error)?;
        Ok(Password::new(
            Some(credential.salt.to_vec()),
            credential.salted_password.to_vec(),
        ))
    }
}

impl fmt::Debug for MetadataPasswordSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MetadataPasswordSource")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AuthSource for MetadataPasswordSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        self.password0(login).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meta::auth::make_password_credential;
    use meta::metadata::{MemoryMetadata, MetadataPutCondition};
    use meta::proto::pb_catalog::{Database, Schema, User};

    #[tokio::test]
    async fn reads_scram_credentials_for_valid_logins() {
        let metadata = Arc::new(MemoryMetadata::new());
        metadata
            .put_user(
                User {
                    name: "alice".to_string(),
                    password: Some(make_password_credential("s3cr3t")),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        metadata
            .put_database(
                Database {
                    name: DEFAULT_DATABASE_NAME.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        metadata
            .put_schema(
                DEFAULT_DATABASE_NAME,
                Schema {
                    name: DEFAULT_SCHEMA_NAME.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let databases = Arc::new(DatabaseHandles::new(metadata.clone()).unwrap());
        let provider = Arc::new(BasicAuthenticationProvider::new(metadata.clone()));
        let source = MetadataPasswordSource::new(provider, metadata, databases);

        let password = source
            .password0(&LoginInfo::new(
                Some("alice"),
                Some(DEFAULT_DATABASE_NAME),
                "127.0.0.1".to_string(),
            ))
            .await
            .unwrap();

        assert!(password.salt().is_some());
        assert_eq!(password.password().len(), 32);
    }

    #[tokio::test]
    async fn rejects_unknown_users_without_exposing_catalog_details() {
        let metadata = Arc::new(MemoryMetadata::new());
        let databases = Arc::new(DatabaseHandles::new(metadata.clone()).unwrap());
        let provider = Arc::new(BasicAuthenticationProvider::new(metadata.clone()));
        let source = MetadataPasswordSource::new(provider, metadata, databases);

        let error = source
            .get_password(&LoginInfo::new(
                Some("missing"),
                Some(DEFAULT_DATABASE_NAME),
                "127.0.0.1".to_string(),
            ))
            .await
            .unwrap_err();

        assert!(matches!(error, PgWireError::InvalidPassword(name) if name == "missing"));
    }
}
