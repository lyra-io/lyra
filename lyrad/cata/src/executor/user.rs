use super::matches_like;
use crate::sql::{AlterUser, AlterUserAction, CreateUser, DropUser, ShowUsers, UserPassword};
use crate::{CataError, Result};
use meta::auth::make_password_credential;
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

        let user = User {
            id: self.metadata.allocate_user_id().await?,
            name: statement.name().to_string(),
            password: Some(make_password_credential(statement.password())),
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
                if self.metadata.get_user(new_name).await?.is_some() {
                    return Err(CataError::UserAlreadyExists(new_name.clone()));
                }
                let mut user = record.value().clone();
                user.name = new_name.clone();
                self.metadata
                    .rename_user(statement.name(), user, record.version())
                    .await
                    .map_err(|error| match error {
                        MetadataError::Conflict(_) => {
                            CataError::UserChanged(statement.name().to_string())
                        }
                        error => error.into(),
                    })?;
            }
            AlterUserAction::Password(password) => {
                let mut user = record.value().clone();
                user.password = Self::password0(password);
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
        let deletes = records
            .iter()
            .map(|(name, version)| (name.clone(), *version))
            .collect::<Vec<_>>();
        self.metadata
            .delete_users(&deletes)
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => CataError::UserChanged(deletes[0].0.clone()),
                error => error.into(),
            })?;
        Ok(DropUserOutcome::Dropped)
    }

    pub async fn show(&self, statement: &ShowUsers) -> Result<Vec<String>> {
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
            .map(|user| user.value().name.clone())
            .collect::<Vec<_>>();
        users.sort();
        Ok(users)
    }

    fn password0(password: &UserPassword) -> Option<PasswordCredential> {
        match password {
            UserPassword::Null => None,
            UserPassword::Value(password) => Some(make_password_credential(password)),
        }
    }
}
