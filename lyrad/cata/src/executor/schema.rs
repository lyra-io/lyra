use super::matches_like;
use crate::sql::{AlterSchema, CreateSchema, DropSchema, ShowSchemas};
use crate::{CataError, Result};
use meta::metadata::{Metadata, MetadataError, MetadataPutCondition};
use meta::proto::pb_catalog::Schema;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateSchemaOutcome {
    Created,
    AlreadyExists,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropSchemaOutcome {
    Dropped,
    NotFound,
}

pub(crate) struct SchemaExecutor {
    // Immutable state
    metadata: Arc<dyn Metadata>,
}

impl SchemaExecutor {
    pub(crate) fn new(metadata: Arc<dyn Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn create(
        &self,
        current_database: &str,
        statement: &CreateSchema,
    ) -> Result<CreateSchemaOutcome> {
        let database = statement.schema().database().unwrap_or(current_database);
        self.require_database0(database).await?;

        if self
            .metadata
            .get_schema(database, statement.schema().name())
            .await?
            .is_some()
        {
            return if statement.if_not_exists() {
                Ok(CreateSchemaOutcome::AlreadyExists)
            } else {
                Err(CataError::SchemaAlreadyExists {
                    database: database.to_string(),
                    schema: statement.schema().name().to_string(),
                })
            };
        }

        let schema = Schema {
            name: statement.schema().name().to_string(),
        };
        match self
            .metadata
            .put_schema(database, schema, MetadataPutCondition::NotExists)
            .await
        {
            Ok(_) => Ok(CreateSchemaOutcome::Created),
            Err(MetadataError::Conflict(_)) if statement.if_not_exists() => {
                Ok(CreateSchemaOutcome::AlreadyExists)
            }
            Err(MetadataError::Conflict(_)) => Err(CataError::SchemaAlreadyExists {
                database: database.to_string(),
                schema: statement.schema().name().to_string(),
            }),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn alter(&self, current_database: &str, statement: &AlterSchema) -> Result<()> {
        let database = statement.schema().database().unwrap_or(current_database);
        self.require_database0(database).await?;
        let Some(record) = self
            .metadata
            .get_schema(database, statement.schema().name())
            .await?
        else {
            return Err(CataError::SchemaNotFound {
                database: database.to_string(),
                schema: statement.schema().name().to_string(),
            });
        };
        if !schema_is_empty(&self.metadata, database, statement.schema().name()).await? {
            return Err(CataError::SchemaNotEmpty {
                database: database.to_string(),
                schema: statement.schema().name().to_string(),
            });
        }

        let mut new_schema = record.value().clone();
        new_schema.name = statement.new_name().to_string();
        let new_version = self
            .metadata
            .put_schema(database, new_schema, MetadataPutCondition::NotExists)
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => CataError::SchemaAlreadyExists {
                    database: database.to_string(),
                    schema: statement.new_name().to_string(),
                },
                error => error.into(),
            })?;

        if let Err(error) = self
            .metadata
            .delete_schema(database, statement.schema().name(), Some(record.version()))
            .await
        {
            let _ = self
                .metadata
                .delete_schema(database, statement.new_name(), Some(new_version))
                .await;
            return Err(match error {
                MetadataError::Conflict(_) => CataError::SchemaChanged {
                    database: database.to_string(),
                    schema: statement.schema().name().to_string(),
                },
                error => error.into(),
            });
        }
        Ok(())
    }

    pub async fn drop(
        &self,
        current_database: &str,
        statement: &DropSchema,
    ) -> Result<DropSchemaOutcome> {
        let database = statement.schema().database().unwrap_or(current_database);
        self.require_database0(database).await?;
        let Some(record) = self
            .metadata
            .get_schema(database, statement.schema().name())
            .await?
        else {
            return if statement.if_exists() {
                Ok(DropSchemaOutcome::NotFound)
            } else {
                Err(CataError::SchemaNotFound {
                    database: database.to_string(),
                    schema: statement.schema().name().to_string(),
                })
            };
        };

        if statement.cascade() {
            drop_schema_contents0(&self.metadata, database, statement.schema().name()).await?;
        } else if !schema_is_empty(&self.metadata, database, statement.schema().name()).await? {
            return Err(CataError::SchemaNotEmpty {
                database: database.to_string(),
                schema: statement.schema().name().to_string(),
            });
        }

        self.metadata
            .delete_schema(database, statement.schema().name(), Some(record.version()))
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => CataError::SchemaChanged {
                    database: database.to_string(),
                    schema: statement.schema().name().to_string(),
                },
                error => error.into(),
            })?;
        Ok(DropSchemaOutcome::Dropped)
    }

    pub async fn show(&self, database: &str, statement: &ShowSchemas) -> Result<Vec<String>> {
        self.require_database0(database).await?;
        let mut schemas = self
            .metadata
            .list_schemas(database)
            .await?
            .into_iter()
            .filter(|schema| {
                statement
                    .like()
                    .is_none_or(|pattern| matches_like(&schema.value().name, pattern))
            })
            .map(|schema| schema.value().name.clone())
            .collect::<Vec<_>>();
        schemas.sort();
        Ok(schemas)
    }

    async fn require_database0(&self, database: &str) -> Result<()> {
        if self.metadata.get_database(database).await?.is_none() {
            return Err(CataError::DatabaseNotFound(database.to_string()));
        }
        Ok(())
    }
}

pub(crate) async fn schema_is_empty(
    metadata: &Arc<dyn Metadata>,
    database: &str,
    schema: &str,
) -> Result<bool> {
    Ok(metadata
        .list_connections(database, schema)
        .await?
        .is_empty()
        && metadata.list_secrets(database, schema).await?.is_empty())
}

async fn drop_schema_contents0(
    metadata: &Arc<dyn Metadata>,
    database: &str,
    schema: &str,
) -> Result<()> {
    for record in metadata.list_connections(database, schema).await? {
        metadata
            .delete_connection(
                database,
                schema,
                &record.value().name,
                Some(record.version()),
            )
            .await?;
    }
    for record in metadata.list_secrets(database, schema).await? {
        metadata
            .delete_secret(
                database,
                schema,
                &record.value().name,
                Some(record.version()),
            )
            .await?;
    }
    Ok(())
}
