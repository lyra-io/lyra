#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedUser {
    name: String,
}

impl AuthenticatedUser {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
