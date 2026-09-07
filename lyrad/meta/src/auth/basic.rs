use crate::auth::{AuthenticatedUser, AuthenticationError, AuthenticationProvider, Result};
use crate::metadata::Metadata;
use crate::proto::pb_catalog::PasswordCredential;
use async_trait::async_trait;
use ring::pbkdf2::{self, PBKDF2_HMAC_SHA256};
use std::borrow::Cow;
use std::num::NonZeroU32;
use std::sync::Arc;

pub const BASIC_PASSWORD_ITERATIONS: u32 = 4096;

const BASIC_PASSWORD_SALT_LENGTH: usize = 18;
const SCRAM_SHA_256_OUTPUT_LENGTH: usize = 32;

pub struct BasicAuthenticationProvider {
    // Immutable state
    metadata: Arc<dyn Metadata>,
}

impl BasicAuthenticationProvider {
    pub fn new(metadata: Arc<dyn Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn password_credential(&self, name: &str) -> Result<PasswordCredential> {
        let user = self
            .metadata
            .get_user(name)
            .await?
            .ok_or(AuthenticationError::InvalidCredentials)?;
        user.value()
            .password
            .clone()
            .filter(valid_password_credential0)
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
            .ok_or(AuthenticationError::InvalidCredentials)?;
        if !valid_password_credential0(credential)
            || !verify_password_credential(password, credential)
        {
            return Err(AuthenticationError::InvalidCredentials);
        }
        Ok(AuthenticatedUser::new(
            user.value().id,
            user.value().name.clone(),
        ))
    }
}

pub fn make_password_credential(password: &str) -> PasswordCredential {
    let salt = rand::random::<[u8; BASIC_PASSWORD_SALT_LENGTH]>();
    PasswordCredential {
        salted_password: derive_password0(password, &salt, BASIC_PASSWORD_ITERATIONS)
            .expect("the basic password iteration count is nonzero")
            .into(),
        salt: salt.to_vec().into(),
        iterations: BASIC_PASSWORD_ITERATIONS,
    }
}

pub fn verify_password_credential(password: &str, credential: &PasswordCredential) -> bool {
    let Some(iterations) = NonZeroU32::new(credential.iterations) else {
        return false;
    };
    if credential.salted_password.len() != SCRAM_SHA_256_OUTPUT_LENGTH {
        return false;
    }
    let normalized = normalize_password0(password);
    pbkdf2::verify(
        PBKDF2_HMAC_SHA256,
        iterations,
        &credential.salt,
        normalized.as_bytes(),
        &credential.salted_password,
    )
    .is_ok()
}

fn derive_password0(password: &str, salt: &[u8], iterations: u32) -> Option<Vec<u8>> {
    let iterations = NonZeroU32::new(iterations)?;
    let normalized = normalize_password0(password);
    let mut salted_password = vec![0; SCRAM_SHA_256_OUTPUT_LENGTH];
    pbkdf2::derive(
        PBKDF2_HMAC_SHA256,
        iterations,
        salt,
        normalized.as_bytes(),
        &mut salted_password,
    );
    Some(salted_password)
}

fn normalize_password0(password: &str) -> Cow<'_, str> {
    stringprep::saslprep(password).unwrap_or(Cow::Borrowed(password))
}

fn valid_password_credential0(credential: &PasswordCredential) -> bool {
    credential.iterations == BASIC_PASSWORD_ITERATIONS
        && credential.salted_password.len() == SCRAM_SHA_256_OUTPUT_LENGTH
        && !credential.salt.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{MemoryMetadata, MetadataPutCondition};
    use crate::proto::pb_catalog::User;

    #[test]
    fn stores_scram_salted_password_material() {
        let first = make_password_credential("s3cr3t");
        let second = make_password_credential("s3cr3t");

        assert_eq!(first.iterations, BASIC_PASSWORD_ITERATIONS);
        assert!(verify_password_credential("s3cr3t", &first));
        assert!(!verify_password_credential("wrong", &first));
        assert_ne!(first.salted_password.as_ref(), b"s3cr3t");
        assert_ne!(first.salt, second.salt);
    }

    #[tokio::test]
    async fn authenticates_a_catalog_user() {
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
        let provider = BasicAuthenticationProvider::new(metadata);

        let user = provider.authenticate("alice", "s3cr3t").await.unwrap();

        assert_eq!(user.id(), 1);
        assert_eq!(user.name(), "alice");
        assert!(matches!(
            provider.authenticate("alice", "wrong").await,
            Err(AuthenticationError::InvalidCredentials)
        ));
    }
}
