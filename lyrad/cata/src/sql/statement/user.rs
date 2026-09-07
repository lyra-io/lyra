use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserStatement {
    Create(CreateUser),
    Alter(AlterUser),
    Drop(DropUser),
    Show(ShowUsers),
}

#[derive(Clone, PartialEq, Eq)]
pub struct CreateUser {
    name: String,
    password: String,
}

impl CreateUser {
    pub fn new(name: String, password: String) -> Self {
        Self { name, password }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn password(&self) -> &str {
        &self.password
    }
}

impl fmt::Debug for CreateUser {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateUser")
            .field("name", &self.name)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum UserPassword {
    Null,
    Value(String),
}

impl fmt::Debug for UserPassword {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => formatter.write_str("Null"),
            Self::Value(_) => formatter.write_str("Value([REDACTED])"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterUserAction {
    Rename(String),
    Password(UserPassword),
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
