use crate::auth::{AuthenticatedUser, AuthenticationError, AuthenticationProvider, Result};
use crate::metadata::Metadata;
use crate::proto::pb_catalog::Scram;
use crate::utils::scram::{as_scram, is_valid_scram, verify_scram};
use async_trait::async_trait;
use std::sync::Arc;

pub struct BasicAuthenticationProvider {
    // Immutable state
    metadata: Arc<dyn Metadata>,
}

impl BasicAuthenticationProvider {
    pub fn new(metadata: Arc<dyn Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn scram(&self, name: &str) -> Result<Scram> {
        let user = self
            .metadata
            .get_user(name)
            .await?
            .ok_or(AuthenticationError::InvalidCredentials)?;
        user.value()
            .password
            .as_ref()
            .and_then(as_scram)
            .filter(|scram| is_valid_scram(scram))
            .cloned()
            .ok_or(AuthenticationError::InvalidCredentials)
    }
}

#[async_trait]
impl AuthenticationProvider for BasicAuthenticationProvider {
    async fn authenticate(&self, name: &str, password: &str) -> Result<AuthenticatedUser> {
        let user = self
            .metadata
            .get_user(name)
            .await?
            .ok_or(AuthenticationError::InvalidCredentials)?;
        let credential = user
            .value()
            .password
            .as_ref()
            .and_then(as_scram)
            .ok_or(AuthenticationError::InvalidCredentials)?;
        if !is_valid_scram(credential) || !verify_scram(password, credential) {
            return Err(AuthenticationError::InvalidCredentials);
        }
        Ok(AuthenticatedUser::new(user.value().name.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{MemoryMetadata, MetadataPutCondition};
    use crate::proto::pb_catalog::User;
    use crate::utils::scram::make_scram_value;

    #[tokio::test]
    async fn authenticates_a_catalog_user() {
        let metadata = Arc::new(MemoryMetadata::new());
        metadata
            .put_user(
                User {
                    name: "alice".to_string(),
                    password: Some(make_scram_value("s3cr3t")),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let provider = BasicAuthenticationProvider::new(metadata);

        let user = provider.authenticate("alice", "s3cr3t").await.unwrap();

        assert_eq!(user.name(), "alice");
        assert!(matches!(
            provider.authenticate("alice", "wrong").await,
            Err(AuthenticationError::InvalidCredentials)
        ));
    }
}
