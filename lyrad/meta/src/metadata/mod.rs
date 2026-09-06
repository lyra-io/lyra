mod api;
mod error;
pub mod oxia;
pub mod path;

pub use api::{Metadata, MetadataPutCondition, MetadataRecord, MetadataVersion};
pub use error::{MetadataError, Result};

pub const DEFAULT_DATABASE_NAME: &str = "dev";
pub const DEFAULT_SCHEMA_NAME: &str = "public";
