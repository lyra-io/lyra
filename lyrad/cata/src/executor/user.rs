use super::matches_like;
use crate::sql::{
    AlterUser, AlterUserAction, CreateUser, DropUser, ShowUsers, UserOptions, UserPassword,
};
use crate::{CataError, Result};
use datafusion_postgres::pgwire::api::auth::sasl::scram::{
    SCRAM_ITERATIONS, gen_salted_password, random_nonce,
};
use meta::metadata::{Metadata, MetadataError, MetadataPutCondition};
use meta::proto::pb_catalog::{PasswordCredential, User};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateUserOutcome {
    Created,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropUserOutcome {
    Dropped,
    NotFound,
}

pub(crate) struct UserSummary {
    name: String,
    superuser: bool,
    create_database: bool,
    create_user: bool,
}

impl UserSummary {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn superuser(&self) -> bool {
        self.superuser
    }

    pub(crate) fn create_database(&self) -> bool {
        self.create_database
    }

    pub(crate) fn create_user(&self) -> bool {
        self.create_user
    }
}

pub(crate) struct UserExecutor {
    // Immutable state
    metadata: Arc<dyn Metadata>,
}

impl UserExecutor {
    pub(crate) fn new(metadata: Arc<dyn Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn create(&self, statement: &CreateUser) -> Result<CreateUserOutcome> {
        if self.metadata.get_user(statement.name()).await?.is_some() {
            return Err(CataError::UserAlreadyExists(statement.name().to_string()));
        }

        let options = statement.options();
        let user = User {
            name: statement.name().to_string(),
            is_superuser: options.superuser().unwrap_or(false),
            can_create_database: options.create_database().unwrap_or(false),
            can_create_user: options.create_user().unwrap_or(false),
            can_login: true,
            password: Self::password0(options.password()),
        };
        self.metadata
            .put_user(user, MetadataPutCondition::NotExists)
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => {
                    CataError::UserAlreadyExists(statement.name().to_string())
                }
                error => error.into(),
            })?;
        Ok(CreateUserOutcome::Created)
    }

    pub async fn alter(&self, current_user: Option<&str>, statement: &AlterUser) -> Result<()> {
        let Some(record) = self.metadata.get_user(statement.name()).await? else {
            return Err(CataError::UserNotFound(statement.name().to_string()));
        };

        match statement.action() {
            AlterUserAction::Rename(new_name) => {
                if current_user == Some(statement.name()) {
                    return Err(CataError::UserInUse(statement.name().to_string()));
                }
                let mut user = record.value().clone();
                user.name = new_name.clone();
                let new_version = self
                    .metadata
                    .put_user(user, MetadataPutCondition::NotExists)
                    .await
                    .map_err(|error| match error {
                        MetadataError::Conflict(_) => {
                            CataError::UserAlreadyExists(new_name.clone())
                        }
                        error => error.into(),
                    })?;
                if let Err(error) = self
                    .metadata
                    .delete_user(statement.name(), Some(record.version()))
                    .await
                {
                    let _ = self.metadata.delete_user(new_name, Some(new_version)).await;
                    return Err(match error {
                        MetadataError::Conflict(_) => {
                            CataError::UserChanged(statement.name().to_string())
                        }
                        error => error.into(),
                    });
                }
            }
            AlterUserAction::Options(options) => {
                let mut user = record.value().clone();
                Self::apply_options0(&mut user, options);
                self.metadata
                    .put_user(user, MetadataPutCondition::Version(record.version()))
                    .await
                    .map_err(|error| match error {
                        MetadataError::Conflict(_) => {
                            CataError::UserChanged(statement.name().to_string())
                        }
                        error => error.into(),
                    })?;
            }
        }
        Ok(())
    }

    pub async fn drop(
        &self,
        current_user: Option<&str>,
        statement: &DropUser,
    ) -> Result<DropUserOutcome> {
        let mut records = Vec::new();
        for name in statement.names() {
            if current_user == Some(name.as_str()) {
                return Err(CataError::UserInUse(name.clone()));
            }
            if records.iter().any(|(existing, _)| existing == name) {
                continue;
            }
            match self.metadata.get_user(name).await? {
                Some(record) => records.push((name.clone(), record.version())),
                None if statement.if_exists() => {}
                None => return Err(CataError::UserNotFound(name.clone())),
            }
        }

        if records.is_empty() {
            return Ok(DropUserOutcome::NotFound);
        }
        for (name, version) in records {
            self.metadata
                .delete_user(&name, Some(version))
                .await
                .map_err(|error| match error {
                    MetadataError::Conflict(_) => CataError::UserChanged(name),
                    error => error.into(),
                })?;
        }
        Ok(DropUserOutcome::Dropped)
    }

    pub async fn show(&self, statement: &ShowUsers) -> Result<Vec<UserSummary>> {
        let mut users = self
            .metadata
            .list_users()
            .await?
            .into_iter()
            .filter(|user| {
                statement
                    .like()
                    .is_none_or(|pattern| matches_like(&user.value().name, pattern))
            })
            .map(|user| UserSummary {
                name: user.value().name.clone(),
                superuser: user.value().is_superuser,
                create_database: user.value().can_create_database,
                create_user: user.value().can_create_user,
            })
            .collect::<Vec<_>>();
        users.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(users)
    }

    fn apply_options0(user: &mut User, options: &UserOptions) {
        if let Some(value) = options.superuser() {
            user.is_superuser = value;
        }
        if let Some(value) = options.create_database() {
            user.can_create_database = value;
        }
        if let Some(value) = options.create_user() {
            user.can_create_user = value;
        }
        match options.password() {
            UserPassword::Unchanged => {}
            UserPassword::Null => user.password = None,
            UserPassword::Value(_) => user.password = Self::password0(options.password()),
        }
    }

    fn password0(password: &UserPassword) -> Option<PasswordCredential> {
        let UserPassword::Value(password) = password else {
            return None;
        };
        let salt = random_nonce().into_bytes();
        Some(PasswordCredential {
            salted_password: gen_salted_password(password, &salt, SCRAM_ITERATIONS).into(),
            salt: salt.into(),
            iterations: SCRAM_ITERATIONS as u32,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_scram_salted_password_material() {
        let password = UserPassword::Value("s3cr3t".to_string());
        let first = UserExecutor::password0(&password).unwrap();
        let second = UserExecutor::password0(&password).unwrap();

        assert_eq!(first.iterations, SCRAM_ITERATIONS as u32);
        assert_eq!(
            first.salted_password.as_ref(),
            gen_salted_password("s3cr3t", &first.salt, SCRAM_ITERATIONS)
        );
        assert_ne!(first.salted_password.as_ref(), b"s3cr3t");
        assert_ne!(first.salt, second.salt);
    }
}
