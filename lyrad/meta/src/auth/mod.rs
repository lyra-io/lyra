mod basic;
mod error;
mod identity;

pub use basic::{
    BASIC_PASSWORD_ITERATIONS, BasicAuthenticationProvider, make_password_credential,
    verify_password_credential,
};
pub use error::{AuthenticationError, Result};
pub use identity::AuthenticatedUser;

use async_trait::async_trait;

#[async_trait]
pub trait AuthenticationProvider: Send + Sync {
    async fn authenticate(&self, name: &str, password: &str) -> Result<AuthenticatedUser>;
}
