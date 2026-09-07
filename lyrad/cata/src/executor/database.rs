use super::{matches_like, schema_is_empty};
use crate::handler::DatabaseHandles;
use crate::sql::{AlterDatabase, CreateDatabase, DropDatabase, ShowDatabases};
use crate::{CataError, Result};
use meta::metadata::{
    DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, Metadata, MetadataError, MetadataPutCondition,
};
use meta::proto::pb_catalog::{Database, Schema};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateDatabaseOutcome {
    Created,
    AlreadyExists,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropDatabaseOutcome {
    Dropped,
    NotFound,
}

pub(crate) struct DatabaseExecutor {
    // Immutable state
    metadata: Arc<dyn Metadata>,
    databases: Arc<DatabaseHandles>,
}

impl DatabaseExecutor {
    pub(crate) fn new(metadata: Arc<dyn Metadata>, databases: Arc<DatabaseHandles>) -> Self {
        Self {
            metadata,
            databases,
        }
    }

    pub async fn create(&self, statement: &CreateDatabase) -> Result<CreateDatabaseOutcome> {
        if self
            .metadata
            .get_database(statement.name())
            .await?
            .is_some()
        {
            return if statement.if_not_exists() {
                Ok(CreateDatabaseOutcome::AlreadyExists)
            } else {
                Err(CataError::DatabaseAlreadyExists(
                    statement.name().to_string(),
                ))
            };
        }

        let database = Database {
            name: statement.name().to_string(),
        };
        let database_version = match self
            .metadata
            .put_database(database, MetadataPutCondition::NotExists)
            .await
        {
            Ok(version) => version,
            Err(MetadataError::Conflict(_)) if statement.if_not_exists() => {
                return Ok(CreateDatabaseOutcome::AlreadyExists);
            }
            Err(MetadataError::Conflict(_)) => {
                return Err(CataError::DatabaseAlreadyExists(
                    statement.name().to_string(),
                ));
            }
            Err(error) => return Err(error.into()),
        };

        let schema = Schema {
            name: DEFAULT_SCHEMA_NAME.to_string(),
        };
        match self
            .metadata
            .put_schema(statement.name(), schema, MetadataPutCondition::NotExists)
            .await
        {
            Ok(_) | Err(MetadataError::Conflict(_)) => {}
            Err(error) => {
                let _ = self
                    .metadata
                    .delete_database(statement.name(), Some(database_version))
                    .await;
                return Err(error.into());
            }
        }
        Ok(CreateDatabaseOutcome::Created)
    }

    pub async fn alter(&self, current_database: &str, statement: &AlterDatabase) -> Result<()> {
        self.require_not_active0(current_database, statement.name())?;
        let Some(record) = self.metadata.get_database(statement.name()).await? else {
            return Err(CataError::DatabaseNotFound(statement.name().to_string()));
        };
        if self
            .metadata
            .get_database(statement.new_name())
            .await?
            .is_some()
        {
            return Err(CataError::DatabaseAlreadyExists(
                statement.new_name().to_string(),
            ));
        }

        let schemas = self.metadata.list_schemas(statement.name()).await?;
        for schema in &schemas {
            if !schema_is_empty(&self.metadata, statement.name(), &schema.value().name).await? {
                return Err(CataError::DatabaseNotEmpty(statement.name().to_string()));
            }
        }

        let mut new_database = record.value().clone();
        new_database.name = statement.new_name().to_string();
        let new_database_version = self
            .metadata
            .put_database(new_database, MetadataPutCondition::NotExists)
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => {
                    CataError::DatabaseAlreadyExists(statement.new_name().to_string())
                }
                error => error.into(),
            })?;

        let mut new_schemas = Vec::new();
        for schema in &schemas {
            match self
                .metadata
                .put_schema(
                    statement.new_name(),
                    schema.value().clone(),
                    MetadataPutCondition::NotExists,
                )
                .await
            {
                Ok(version) => new_schemas.push((schema.value().name.clone(), version)),
                Err(error) => {
                    for (name, version) in new_schemas {
                        let _ = self
                            .metadata
                            .delete_schema(statement.new_name(), &name, Some(version))
                            .await;
                    }
                    let _ = self
                        .metadata
                        .delete_database(statement.new_name(), Some(new_database_version))
                        .await;
                    return Err(error.into());
                }
            }
        }

        for schema in schemas {
            self.metadata
                .delete_schema(
                    statement.name(),
                    &schema.value().name,
                    Some(schema.version()),
                )
                .await
                .map_err(|error| match error {
                    MetadataError::Conflict(_) => {
                        CataError::DatabaseChanged(statement.name().to_string())
                    }
                    error => error.into(),
                })?;
        }
        self.metadata
            .delete_database(statement.name(), Some(record.version()))
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => {
                    CataError::DatabaseChanged(statement.name().to_string())
                }
                error => error.into(),
            })?;
        self.databases.remove(statement.name());
        Ok(())
    }

    pub async fn drop(
        &self,
        current_database: &str,
        statement: &DropDatabase,
    ) -> Result<DropDatabaseOutcome> {
        self.require_not_active0(current_database, statement.name())?;
        let Some(record) = self.metadata.get_database(statement.name()).await? else {
            return if statement.if_exists() {
                Ok(DropDatabaseOutcome::NotFound)
            } else {
                Err(CataError::DatabaseNotFound(statement.name().to_string()))
            };
        };
        if !self
            .metadata
            .list_schemas(statement.name())
            .await?
            .is_empty()
        {
            return Err(CataError::DatabaseNotEmpty(statement.name().to_string()));
        }

        self.metadata
            .delete_database(statement.name(), Some(record.version()))
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => {
                    CataError::DatabaseChanged(statement.name().to_string())
                }
                error => error.into(),
            })?;
        self.databases.remove(statement.name());
        Ok(DropDatabaseOutcome::Dropped)
    }

    pub async fn show(&self, statement: &ShowDatabases) -> Result<Vec<String>> {
        let mut databases = self
            .metadata
            .list_databases()
            .await?
            .into_iter()
            .filter(|database| {
                statement
                    .like()
                    .is_none_or(|pattern| matches_like(&database.value().name, pattern))
            })
            .map(|database| database.value().name.clone())
            .collect::<Vec<_>>();
        databases.sort();
        Ok(databases)
    }

    fn require_not_active0(&self, current_database: &str, database: &str) -> Result<()> {
        if database == current_database || database == DEFAULT_DATABASE_NAME {
            return Err(CataError::DatabaseInUse(database.to_string()));
        }
        Ok(())
    }
}
