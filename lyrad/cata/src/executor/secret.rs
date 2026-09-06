use crate::sql::CreateSecret;
use crate::{CataError, Result};
use meta::metadata::{Metadata, MetadataError, MetadataPutCondition};
use meta::proto::pb_catalog::Secret;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateSecretOutcome {
    Created,
    AlreadyExists,
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
        schema: &str,
        statement: &CreateSecret,
    ) -> Result<CreateSecretOutcome> {
        if self
            .metadata
            .get_secret(database, schema, statement.name())
            .await?
            .is_some()
        {
            return if statement.if_not_exists() {
                Ok(CreateSecretOutcome::AlreadyExists)
            } else {
                Err(CataError::SecretAlreadyExists(statement.name().to_string()))
            };
        }

        let secret = Secret {
            name: statement.name().to_string(),
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
            Err(MetadataError::Conflict(_)) => {
                Err(CataError::SecretAlreadyExists(statement.name().to_string()))
            }
            Err(error) => Err(error.into()),
        }
    }
}
