use super::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserStatement {
    Create(CreateUser),
    Alter(AlterUser),
    Drop(DropUser),
    Show(ShowUsers),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateUser {
    name: String,
    password: Value,
}

impl CreateUser {
    pub fn new(name: String, password: Value) -> Self {
        Self { name, password }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn password(&self) -> &Value {
        &self.password
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterUserAction {
    Rename(String),
    Password(Value),
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
