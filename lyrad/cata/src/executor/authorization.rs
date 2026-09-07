use crate::sql::{AlterUserAction, CatalogStatement, DatabaseStatement, UserStatement};
use crate::{CataError, Result};
use meta::metadata::Metadata;
use meta::proto::pb_catalog::User;
use std::sync::Arc;

pub(crate) struct Authorization {
    // Immutable state
    metadata: Arc<dyn Metadata>,
}

impl Authorization {
    pub(crate) fn new(metadata: Arc<dyn Metadata>) -> Self {
        Self { metadata }
    }

    pub(crate) async fn check(
        &self,
        current_user: Option<&str>,
        statement: &CatalogStatement,
    ) -> Result<User> {
        let user = self.user0(current_user).await?;
        match statement {
            CatalogStatement::User(UserStatement::Create(statement)) => {
                Self::require_create_user0(&user)?;
                if statement.options().superuser() == Some(true) {
                    Self::require_superuser0(&user)?;
                }
            }
            CatalogStatement::User(UserStatement::Alter(statement)) => {
                let target = self
                    .metadata
                    .get_user(statement.name())
                    .await?
                    .map(|record| record.value().clone());
                let changes_superuser = matches!(
                    statement.action(),
                    AlterUserAction::Options(options) if options.superuser().is_some()
                );
                if target.is_some_and(|target| target.is_superuser) || changes_superuser {
                    Self::require_superuser0(&user)?;
                    return Ok(user);
                }
                let changes_own_password = statement.name() == user.name
                    && matches!(
                        statement.action(),
                        AlterUserAction::Options(options) if !options.changes_privileges()
                    );
                if !changes_own_password {
                    Self::require_create_user0(&user)?;
                }
            }
            CatalogStatement::User(UserStatement::Drop(statement)) => {
                Self::require_create_user0(&user)?;
                for name in statement.names() {
                    if self
                        .metadata
                        .get_user(name)
                        .await?
                        .is_some_and(|record| record.value().is_superuser)
                    {
                        Self::require_superuser0(&user)?;
                    }
                }
            }
            CatalogStatement::Database(
                DatabaseStatement::Create(_)
                | DatabaseStatement::Alter(_)
                | DatabaseStatement::Drop(_),
            ) => {
                if !user.is_superuser && !user.can_create_database {
                    return Err(CataError::PermissionDenied("manage databases".to_string()));
                }
            }
            CatalogStatement::Database(DatabaseStatement::Show(_))
            | CatalogStatement::Schema(_)
            | CatalogStatement::Secret(_)
            | CatalogStatement::User(UserStatement::Show(_)) => {}
        }
        Ok(user)
    }

    async fn user0(&self, current_user: Option<&str>) -> Result<User> {
        let name = current_user.ok_or(CataError::AuthenticationRequired)?;
        self.metadata
            .get_user(name)
            .await?
            .map(|record| record.value().clone())
            .ok_or(CataError::AuthenticationRequired)
    }

    fn require_create_user0(user: &User) -> Result<()> {
        if user.is_superuser || user.can_create_user {
            Ok(())
        } else {
            Err(CataError::PermissionDenied("manage users".to_string()))
        }
    }

    fn require_superuser0(user: &User) -> Result<()> {
        if user.is_superuser {
            Ok(())
        } else {
            Err(CataError::PermissionDenied("manage superusers".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parse_catalog_statement;
    use meta::metadata::{MemoryMetadata, MetadataPutCondition};
    use meta::proto::pb_catalog::{Database, User};

    async fn put_user0(metadata: &MemoryMetadata, name: &str, can_create_user: bool) {
        metadata
            .put_user(
                User {
                    id: metadata.allocate_user_id().await.unwrap(),
                    name: name.to_string(),
                    is_superuser: false,
                    can_create_database: false,
                    can_create_user,
                    password: None,
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn enforces_user_and_database_privileges() {
        let metadata = Arc::new(MemoryMetadata::new());
        put_user0(&metadata, "manager", true).await;
        put_user0(&metadata, "reader", false).await;
        metadata
            .put_database(
                Database {
                    name: "dev".to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let authorization = Authorization::new(metadata);
        let create_user = parse_catalog_statement("CREATE USER bob").unwrap().unwrap();
        let drop_database = parse_catalog_statement("DROP DATABASE dev")
            .unwrap()
            .unwrap();

        authorization
            .check(Some("manager"), &create_user)
            .await
            .unwrap();
        assert!(matches!(
            authorization.check(Some("reader"), &create_user).await,
            Err(CataError::PermissionDenied(_))
        ));
        assert!(matches!(
            authorization.check(Some("reader"), &drop_database).await,
            Err(CataError::PermissionDenied(_))
        ));
    }

    #[tokio::test]
    async fn allows_users_to_change_their_own_password() {
        let metadata = Arc::new(MemoryMetadata::new());
        put_user0(&metadata, "reader", false).await;
        let authorization = Authorization::new(metadata);
        let statement = parse_catalog_statement("ALTER USER reader PASSWORD 'new-password'")
            .unwrap()
            .unwrap();

        authorization
            .check(Some("reader"), &statement)
            .await
            .unwrap();
    }
}
