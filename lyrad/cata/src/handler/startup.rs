use super::DatabaseHandles;
use crate::error::to_pgwire_error;
use crate::{CataError, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion_postgres::pgwire::api::auth::sasl::SASLAuthStartupHandler;
use datafusion_postgres::pgwire::api::auth::sasl::scram::{SCRAM_ITERATIONS, ScramAuth};
use datafusion_postgres::pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, ServerParameterProvider,
};
use datafusion_postgres::pgwire::api::{ClientInfo, ConnectionManager, METADATA_USER};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use meta::metadata::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, Metadata};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub(crate) struct StartupHandler {
    // Immutable state
    connection_manager: Arc<ConnectionManager>,
    auth_source: Arc<UserAuthSource>,
    parameter_provider: Arc<UserParameterProvider>,
}

impl StartupHandler {
    pub(crate) fn new(
        connection_manager: Arc<ConnectionManager>,
        metadata: Arc<dyn Metadata>,
        databases: Arc<DatabaseHandles>,
    ) -> Self {
        let authenticated_users = Arc::new(DashMap::new());
        let auth_source = Arc::new(UserAuthSource::new(
            metadata,
            databases,
            Arc::clone(&authenticated_users),
        ));
        let parameter_provider = Arc::new(UserParameterProvider::new(authenticated_users));
        Self {
            connection_manager,
            auth_source,
            parameter_provider,
        }
    }

    pub(crate) fn new_handler(&self) -> SASLAuthStartupHandler<UserParameterProvider> {
        let auth_source: Arc<dyn AuthSource> = self.auth_source.clone();
        let mut scram = ScramAuth::new(auth_source);
        scram.set_iterations(SCRAM_ITERATIONS);
        SASLAuthStartupHandler::new(Arc::clone(&self.parameter_provider))
            .with_scram(scram)
            .with_connection_manager(Arc::clone(&self.connection_manager))
    }
}

struct UserAuthSource {
    // Immutable state
    metadata: Arc<dyn Metadata>,
    databases: Arc<DatabaseHandles>,
    authenticated_users: Arc<DashMap<String, bool>>,
}

impl UserAuthSource {
    fn new(
        metadata: Arc<dyn Metadata>,
        databases: Arc<DatabaseHandles>,
        authenticated_users: Arc<DashMap<String, bool>>,
    ) -> Self {
        Self {
            metadata,
            databases,
            authenticated_users,
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
        self.authenticated_users
            .insert(name.to_string(), user.is_superuser);
        Ok(Password::new(
            Some(credential.salt.to_vec()),
            credential.salted_password.to_vec(),
        ))
    }
}

impl fmt::Debug for UserAuthSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UserAuthSource")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AuthSource for UserAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        self.password0(login).await.map_err(|error| match error {
            CataError::UserNotFound(name) | CataError::InvalidPasswordCredential(name) => {
                PgWireError::InvalidPassword(name)
            }
            error => to_pgwire_error(error),
        })
    }
}

pub(crate) struct UserParameterProvider {
    // Immutable state
    default: DefaultServerParameterProvider,
    authenticated_users: Arc<DashMap<String, bool>>,
}

impl UserParameterProvider {
    fn new(authenticated_users: Arc<DashMap<String, bool>>) -> Self {
        Self {
            default: DefaultServerParameterProvider::default(),
            authenticated_users,
        }
    }
}

impl fmt::Debug for UserParameterProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UserParameterProvider")
            .finish_non_exhaustive()
    }
}

impl ServerParameterProvider for UserParameterProvider {
    fn server_parameters<C>(&self, client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let mut parameters = self.default.server_parameters(client)?;
        let is_superuser = client
            .metadata()
            .get(METADATA_USER)
            .and_then(|name| self.authenticated_users.get(name))
            .is_some_and(|value| *value);
        parameters.insert(
            "is_superuser".to_string(),
            if is_superuser { "on" } else { "off" }.to_string(),
        );
        Some(parameters)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::make_password_credential;
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
                    is_superuser: false,
                    can_create_database: false,
                    can_create_user: false,
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
        let authenticated_users = Arc::new(DashMap::new());
        let source = UserAuthSource::new(metadata, databases, Arc::clone(&authenticated_users));

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
        assert_eq!(authenticated_users.get("alice").as_deref(), Some(&false));
    }

    #[tokio::test]
    async fn rejects_unknown_users_without_exposing_catalog_details() {
        let metadata = Arc::new(MemoryMetadata::new());
        let databases = Arc::new(DatabaseHandles::new(metadata.clone()).unwrap());
        let source = UserAuthSource::new(metadata, databases, Arc::new(DashMap::new()));

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
