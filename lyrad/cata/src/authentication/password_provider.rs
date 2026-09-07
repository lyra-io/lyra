use crate::authentication::{AuthenticationHandler, AuthenticationProvider};
use crate::error::to_pgwire_error;
use crate::handler::DatabaseHandles;
use crate::{CataError, Result};
use async_trait::async_trait;
use datafusion_postgres::pgwire::api::auth::sasl::scram::{SCRAM_ITERATIONS, ScramAuth};
use datafusion_postgres::pgwire::api::auth::{AuthSource, LoginInfo, Password};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use meta::metadata::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, Metadata};
use std::fmt;
use std::sync::Arc;

pub(crate) struct PasswordAuthenticationProvider {
    // Immutable state
    source: Arc<MetadataPasswordSource>,
}

impl PasswordAuthenticationProvider {
    pub(crate) fn new(metadata: Arc<dyn Metadata>, databases: Arc<DatabaseHandles>) -> Self {
        Self {
            source: Arc::new(MetadataPasswordSource::new(metadata, databases)),
        }
    }
}

impl AuthenticationProvider for PasswordAuthenticationProvider {
    fn configure(&self, handler: AuthenticationHandler) -> AuthenticationHandler {
        let source: Arc<dyn AuthSource> = self.source.clone();
        let mut scram = ScramAuth::new(source);
        scram.set_iterations(SCRAM_ITERATIONS);
        handler.with_scram(scram)
    }
}

struct MetadataPasswordSource {
    // Immutable state
    metadata: Arc<dyn Metadata>,
    databases: Arc<DatabaseHandles>,
}

impl MetadataPasswordSource {
    fn new(metadata: Arc<dyn Metadata>, databases: Arc<DatabaseHandles>) -> Self {
        Self {
            metadata,
            databases,
        }
    }

    async fn password0(&self, login: &LoginInfo<'_>) -> Result<Password> {
        let name = login.user().ok_or(CataError::AuthenticationRequired)?;
        let user = self
            .metadata
            .get_user(name)
            .await?
            .map(|record| record.value().clone())
            .ok_or_else(|| CataError::UserNotFound(name.to_string()))?;
        let credential = user
            .password
            .ok_or_else(|| CataError::InvalidPasswordCredential(name.to_string()))?;
        if credential.iterations != SCRAM_ITERATIONS as u32 {
            return Err(CataError::InvalidPasswordCredential(name.to_string()));
        }

        let database = login.database().unwrap_or(DEFAULT_DATABASE_NAME);
        if self.metadata.get_database(database).await?.is_none() {
            return Err(CataError::DatabaseNotFound(database.to_string()));
        }
        if self
            .metadata
            .get_schema(database, DEFAULT_SCHEMA_NAME)
            .await?
            .is_none()
        {
            return Err(CataError::SchemaNotFound {
                database: database.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
            });
        }
        self.databases.get(database)?;
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
        self.password0(login).await.map_err(|error| match error {
            CataError::UserNotFound(name) | CataError::InvalidPasswordCredential(name) => {
                PgWireError::InvalidPassword(name)
            }
            error => to_pgwire_error(error),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::make_password_credential;
    use meta::metadata::{MemoryMetadata, MetadataPutCondition};
    use meta::proto::pb_catalog::{Database, Schema, User};

    #[tokio::test]
    async fn reads_scram_credentials_for_valid_logins() {
        let metadata = Arc::new(MemoryMetadata::new());
        metadata
            .put_user(
                User {
                    id: 1,
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
        let source = MetadataPasswordSource::new(metadata, databases);

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
        let source = MetadataPasswordSource::new(metadata, databases);

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
