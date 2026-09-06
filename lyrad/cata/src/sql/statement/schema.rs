#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaStatement {
    Create(CreateSchema),
    Alter(AlterSchema),
    Drop(DropSchema),
    Show(ShowSchemas),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaName {
    database: Option<String>,
    name: String,
}

impl SchemaName {
    pub fn new(database: Option<String>, name: String) -> Self {
        Self { database, name }
    }

    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSchema {
    schema: SchemaName,
    if_not_exists: bool,
}

impl CreateSchema {
    pub fn new(schema: SchemaName, if_not_exists: bool) -> Self {
        Self {
            schema,
            if_not_exists,
        }
    }

    pub fn schema(&self) -> &SchemaName {
        &self.schema
    }

    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterSchema {
    schema: SchemaName,
    new_name: String,
}

impl AlterSchema {
    pub fn new(schema: SchemaName, new_name: String) -> Self {
        Self { schema, new_name }
    }

    pub fn schema(&self) -> &SchemaName {
        &self.schema
    }

    pub fn new_name(&self) -> &str {
        &self.new_name
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropSchema {
    schema: SchemaName,
    if_exists: bool,
    cascade: bool,
}

impl DropSchema {
    pub fn new(schema: SchemaName, if_exists: bool, cascade: bool) -> Self {
        Self {
            schema,
            if_exists,
            cascade,
        }
    }

    pub fn schema(&self) -> &SchemaName {
        &self.schema
    }

    pub fn if_exists(&self) -> bool {
        self.if_exists
    }

    pub fn cascade(&self) -> bool {
        self.cascade
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowSchemas {
    like: Option<String>,
}

impl ShowSchemas {
    pub fn new(like: Option<String>) -> Self {
        Self { like }
    }

    pub fn like(&self) -> Option<&str> {
        self.like.as_deref()
    }
}
