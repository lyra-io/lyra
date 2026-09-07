use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserStatement {
    Create(CreateUser),
    Alter(AlterUser),
    Drop(DropUser),
    Show(ShowUsers),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UserOptions {
    superuser: Option<bool>,
    create_database: Option<bool>,
    create_user: Option<bool>,
    password: UserPassword,
}

impl UserOptions {
    pub fn new(
        superuser: Option<bool>,
        create_database: Option<bool>,
        create_user: Option<bool>,
        password: UserPassword,
    ) -> Self {
        Self {
            superuser,
            create_database,
            create_user,
            password,
        }
    }

    pub fn superuser(&self) -> Option<bool> {
        self.superuser
    }

    pub fn create_database(&self) -> Option<bool> {
        self.create_database
    }

    pub fn create_user(&self) -> Option<bool> {
        self.create_user
    }

    pub fn password(&self) -> &UserPassword {
        &self.password
    }

    pub fn changes_privileges(&self) -> bool {
        self.superuser.is_some() || self.create_database.is_some() || self.create_user.is_some()
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub enum UserPassword {
    #[default]
    Unchanged,
    Null,
    Value(String),
}

impl fmt::Debug for UserPassword {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unchanged => formatter.write_str("Unchanged"),
            Self::Null => formatter.write_str("Null"),
            Self::Value(_) => formatter.write_str("Value([REDACTED])"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateUser {
    name: String,
    options: UserOptions,
}

impl CreateUser {
    pub fn new(name: String, options: UserOptions) -> Self {
        Self { name, options }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn options(&self) -> &UserOptions {
        &self.options
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterUserAction {
    Rename(String),
    Options(UserOptions),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterUser {
    name: String,
    action: AlterUserAction,
}

impl AlterUser {
    pub fn new(name: String, action: AlterUserAction) -> Self {
        Self { name, action }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn action(&self) -> &AlterUserAction {
        &self.action
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropUser {
    names: Vec<String>,
    if_exists: bool,
}

impl DropUser {
    pub fn new(names: Vec<String>, if_exists: bool) -> Self {
        Self { names, if_exists }
    }

    pub fn names(&self) -> &[String] {
        &self.names
    }

    pub fn if_exists(&self) -> bool {
        self.if_exists
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowUsers {
    like: Option<String>,
}

impl ShowUsers {
    pub fn new(like: Option<String>) -> Self {
        Self { like }
    }

    pub fn like(&self) -> Option<&str> {
        self.like.as_deref()
    }
}
