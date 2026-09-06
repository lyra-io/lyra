use super::matches_like;
use crate::sql::{AlterSecret, CreateSecret, DropSecret, SecretName, ShowSecrets};
use crate::{CataError, Result};
use meta::metadata::{
    DEFAULT_SCHEMA_NAME, Metadata, MetadataError, MetadataPutCondition, MetadataRecord,
};
use meta::proto::pb_catalog::Secret;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateSecretOutcome {
    Created,
    AlreadyExists,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropSecretOutcome {
    Dropped,
    NotFound,
}

pub(crate) struct SecretExecutor {
    // Immutable state
    metadata: Arc<dyn Metadata>,
}

impl SecretExecutor {
    pub(crate) fn new(metadata: Arc<dyn Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn create(
        &self,
        database: &str,
        schemas: &[String],
        statement: &CreateSecret,
    ) -> Result<CreateSecretOutcome> {
        let schema = Self::target_schema0(schemas, statement.secret());
        self.require_schema0(database, schema).await?;

        if self
            .metadata
            .get_secret(database, schema, statement.secret().name())
            .await?
            .is_some()
        {
            return if statement.if_not_exists() {
                Ok(CreateSecretOutcome::AlreadyExists)
            } else {
                Err(CataError::SecretAlreadyExists(
                    statement.secret().name().to_string(),
                ))
            };
        }

        let secret = Secret {
            name: statement.secret().name().to_string(),
            value: statement.value().to_vec().into(),
        };
        match self
            .metadata
            .put_secret(database, schema, secret, MetadataPutCondition::NotExists)
            .await
        {
            Ok(_) => Ok(CreateSecretOutcome::Created),
            Err(MetadataError::Conflict(_)) if statement.if_not_exists() => {
                Ok(CreateSecretOutcome::AlreadyExists)
            }
            Err(MetadataError::Conflict(_)) => Err(CataError::SecretAlreadyExists(
                statement.secret().name().to_string(),
            )),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn alter(
        &self,
        database: &str,
        schemas: &[String],
        statement: &AlterSecret,
    ) -> Result<()> {
        let Some((schema, record)) = self.resolve0(database, schemas, statement.secret()).await?
        else {
            return Err(CataError::SecretNotFound(
                statement.secret().name().to_string(),
            ));
        };
        let secret = Secret {
            name: statement.secret().name().to_string(),
            value: statement.value().to_vec().into(),
        };

        self.metadata
            .put_secret(
                database,
                &schema,
                secret,
                MetadataPutCondition::Version(record.version()),
            )
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => {
                    CataError::SecretChanged(statement.secret().name().to_string())
                }
                error => error.into(),
            })?;
        Ok(())
    }

    pub async fn drop(
        &self,
        database: &str,
        schemas: &[String],
        statement: &DropSecret,
    ) -> Result<DropSecretOutcome> {
        let Some((schema, record)) = self.resolve0(database, schemas, statement.secret()).await?
        else {
            return if statement.if_exists() {
                Ok(DropSecretOutcome::NotFound)
            } else {
                Err(CataError::SecretNotFound(
                    statement.secret().name().to_string(),
                ))
            };
        };

        let qualified_name = format!("{schema}.{}", statement.secret().name());
        if let Some(connection) = self
            .metadata
            .list_connections(database, &schema)
            .await?
            .into_iter()
            .find(|connection| {
                connection.value().secret_refs.iter().any(|secret| {
                    secret.data == statement.secret().name() || secret.data == qualified_name
                })
            })
        {
            return Err(CataError::SecretInUse {
                secret: qualified_name,
                connection: connection.value().name.clone(),
            });
        }

        self.metadata
            .delete_secret(
                database,
                &schema,
                statement.secret().name(),
                Some(record.version()),
            )
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => {
                    CataError::SecretChanged(statement.secret().name().to_string())
                }
                error => error.into(),
            })?;
        Ok(DropSecretOutcome::Dropped)
    }

    pub async fn show(
        &self,
        database: &str,
        schemas: &[String],
        statement: &ShowSecrets,
    ) -> Result<Vec<String>> {
        let schemas = if let Some(schema) = statement.schema() {
            self.require_schema0(database, schema).await?;
            vec![schema.to_string()]
        } else {
            schemas.to_vec()
        };

        let mut secrets = Vec::new();
        for schema in schemas {
            if self.metadata.get_schema(database, &schema).await?.is_none() {
                continue;
            }
            secrets.extend(
                self.metadata
                    .list_secrets(database, &schema)
                    .await?
                    .into_iter()
                    .filter(|secret| {
                        statement
                            .like()
                            .is_none_or(|pattern| matches_like(&secret.value().name, pattern))
                    })
                    .map(|secret| format!("{schema}.{}", secret.value().name)),
            );
        }
        secrets.sort();
        Ok(secrets)
    }

    async fn resolve0(
        &self,
        database: &str,
        schemas: &[String],
        secret: &SecretName,
    ) -> Result<Option<(String, MetadataRecord<Secret>)>> {
        if let Some(schema) = secret.schema() {
            self.require_schema0(database, schema).await?;
            return Ok(self
                .metadata
                .get_secret(database, schema, secret.name())
                .await?
                .map(|record| (schema.to_string(), record)));
        }

        for schema in schemas {
            if let Some(record) = self
                .metadata
                .get_secret(database, schema, secret.name())
                .await?
            {
                return Ok(Some((schema.clone(), record)));
            }
        }
        Ok(None)
    }

    async fn require_schema0(&self, database: &str, schema: &str) -> Result<()> {
        if self.metadata.get_schema(database, schema).await?.is_none() {
            return Err(CataError::SchemaNotFound {
                database: database.to_string(),
                schema: schema.to_string(),
            });
        }
        Ok(())
    }

    fn target_schema0<'a>(schemas: &'a [String], secret: &'a SecretName) -> &'a str {
        secret
            .schema()
            .or_else(|| schemas.first().map(String::as_str))
            .unwrap_or(DEFAULT_SCHEMA_NAME)
    }
}
