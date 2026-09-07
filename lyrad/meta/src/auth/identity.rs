#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedUser {
    id: u32,
    name: String,
}

impl AuthenticatedUser {
    pub fn new(id: u32, name: String) -> Self {
        Self { id, name }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
