#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DatabaseStatement {
    Create(CreateDatabase),
    Alter(AlterDatabase),
    Drop(DropDatabase),
    Show(ShowDatabases),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDatabase {
    name: String,
    if_not_exists: bool,
}

impl CreateDatabase {
    pub fn new(name: String, if_not_exists: bool) -> Self {
        Self {
            name,
            if_not_exists,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterDatabase {
    name: String,
    new_name: String,
}

impl AlterDatabase {
    pub fn new(name: String, new_name: String) -> Self {
        Self { name, new_name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn new_name(&self) -> &str {
        &self.new_name
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropDatabase {
    name: String,
    if_exists: bool,
}

impl DropDatabase {
    pub fn new(name: String, if_exists: bool) -> Self {
        Self { name, if_exists }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn if_exists(&self) -> bool {
        self.if_exists
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowDatabases {
    like: Option<String>,
}

impl ShowDatabases {
    pub fn new(like: Option<String>) -> Self {
        Self { like }
    }

    pub fn like(&self) -> Option<&str> {
        self.like.as_deref()
    }
}
