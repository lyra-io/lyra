use crate::config::PostgresOptions;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CataOptions {
    postgres: PostgresOptions,
}

impl CataOptions {
    pub fn new(postgres: PostgresOptions) -> Self {
        Self { postgres }
    }

    pub fn postgres(&self) -> &PostgresOptions {
        &self.postgres
    }
}
