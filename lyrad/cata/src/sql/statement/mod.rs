mod database;
mod schema;
mod secret;
mod user;

pub use database::{AlterDatabase, CreateDatabase, DatabaseStatement, DropDatabase, ShowDatabases};
pub use schema::{AlterSchema, CreateSchema, DropSchema, SchemaName, SchemaStatement, ShowSchemas};
pub use secret::{AlterSecret, CreateSecret, DropSecret, SecretName, SecretStatement, ShowSecrets};
pub use user::{
    AlterUser, AlterUserAction, CreateUser, DropUser, ShowUsers, UserPassword, UserStatement,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogStatement {
    Database(DatabaseStatement),
    Schema(SchemaStatement),
    Secret(SecretStatement),
    User(UserStatement),
}
