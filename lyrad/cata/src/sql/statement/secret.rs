#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecretStatement {
    Create(CreateSecret),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSecret {
    name: String,
    value: Vec<u8>,
    if_not_exists: bool,
}

impl CreateSecret {
    pub fn new(name: String, value: Vec<u8>, if_not_exists: bool) -> Self {
        Self {
            name,
            value,
            if_not_exists,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }
}
