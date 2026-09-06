mod secret;

pub use secret::{AlterSecret, CreateSecret, DropSecret, SecretName, SecretStatement, ShowSecrets};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogStatement {
    Secret(SecretStatement),
}
