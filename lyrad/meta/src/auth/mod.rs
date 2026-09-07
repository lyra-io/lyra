mod basic;
mod error;
mod identity;

pub use basic::BasicAuthenticationProvider;
pub use error::{AuthenticationError, Result};
pub use identity::AuthenticatedUser;

use async_trait::async_trait;

#[async_trait]
pub trait AuthenticationProvider: Send + Sync {
    async fn authenticate(&self, name: &str, password: &str) -> Result<AuthenticatedUser>;
}
