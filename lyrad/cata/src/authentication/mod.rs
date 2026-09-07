mod password;
mod password_provider;

pub(crate) use password::make_password_credential;
pub(crate) use password_provider::PasswordAuthenticationProvider;

use datafusion_postgres::pgwire::api::auth::DefaultServerParameterProvider;
use datafusion_postgres::pgwire::api::auth::sasl::SASLAuthStartupHandler;

pub(crate) type AuthenticationHandler = SASLAuthStartupHandler<DefaultServerParameterProvider>;

pub(crate) trait AuthenticationProvider: Send + Sync {
    fn configure(&self, handler: AuthenticationHandler) -> AuthenticationHandler;
}
