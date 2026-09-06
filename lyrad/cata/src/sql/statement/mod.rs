mod database;
mod schema;
mod secret;

pub use database::{AlterDatabase, CreateDatabase, DatabaseStatement, DropDatabase, ShowDatabases};
pub use schema::{AlterSchema, CreateSchema, DropSchema, SchemaName, SchemaStatement, ShowSchemas};
pub use secret::{AlterSecret, CreateSecret, DropSecret, SecretName, SecretStatement, ShowSecrets};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogStatement {
    Database(DatabaseStatement),
    Schema(SchemaStatement),
    Secret(SecretStatement),
}
