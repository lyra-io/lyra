pub mod error;

use chronicle_catalog::{Action, CatalogRef, Dataset, Versioned};
use error::LensError;

pub struct Lens {
    catalog: CatalogRef,
}

impl Lens {
    pub fn new(catalog: CatalogRef) -> Self {
        Self { catalog }
    }

    pub async fn execute(&self, sql: &str) -> Result<LensOutput, LensError> {
        let statement = sql.trim().trim_end_matches(';').trim();
        if statement.eq_ignore_ascii_case("show datasets") {
            return Ok(LensOutput::Datasets(self.catalog.list_datasets().await?));
        }

        Err(LensError::UnsupportedStatement(statement.to_string()))
    }
}

#[derive(Debug, Clone)]
pub enum LensOutput {
    Empty,
    Message(String),
    Datasets(Vec<Versioned<Dataset>>),
    Action(Versioned<Action>),
}
