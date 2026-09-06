mod secret;

pub use secret::{CreateSecret, SecretStatement};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogStatement {
    Secret(SecretStatement),
}
