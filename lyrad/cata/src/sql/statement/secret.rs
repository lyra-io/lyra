#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecretStatement {
    Create(CreateSecret),
    Alter(AlterSecret),
    Drop(DropSecret),
    Show(ShowSecrets),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretName {
    schema: Option<String>,
    name: String,
}

impl SecretName {
    pub fn new(schema: Option<String>, name: String) -> Self {
        Self { schema, name }
    }

    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSecret {
    secret: SecretName,
    value: Vec<u8>,
    if_not_exists: bool,
}

impl CreateSecret {
    pub fn new(secret: SecretName, value: Vec<u8>, if_not_exists: bool) -> Self {
        Self {
            secret,
            value,
            if_not_exists,
        }
    }

    pub fn secret(&self) -> &SecretName {
        &self.secret
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterSecret {
    secret: SecretName,
    value: Vec<u8>,
}

impl AlterSecret {
    pub fn new(secret: SecretName, value: Vec<u8>) -> Self {
        Self { secret, value }
    }

    pub fn secret(&self) -> &SecretName {
        &self.secret
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropSecret {
    secret: SecretName,
    if_exists: bool,
}

impl DropSecret {
    pub fn new(secret: SecretName, if_exists: bool) -> Self {
        Self { secret, if_exists }
    }

    pub fn secret(&self) -> &SecretName {
        &self.secret
    }

    pub fn if_exists(&self) -> bool {
        self.if_exists
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowSecrets {
    schema: Option<String>,
    like: Option<String>,
}

impl ShowSecrets {
    pub fn new(schema: Option<String>, like: Option<String>) -> Self {
        Self { schema, like }
    }

    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub fn like(&self) -> Option<&str> {
        self.like.as_deref()
    }
}
