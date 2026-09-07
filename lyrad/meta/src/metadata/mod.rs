mod api;
mod error;
mod memory;
pub mod oxia;
pub mod path;

pub use api::{Metadata, MetadataPutCondition, MetadataRecord, MetadataVersion};
pub use error::{MetadataError, Result};
pub use memory::MemoryMetadata;

pub const DEFAULT_DATABASE_NAME: &str = "dev";
pub const DEFAULT_SCHEMA_NAME: &str = "public";
