use crate::metadata::Result;
use crate::proto::pb_catalog::{Connection, Database, Schema, Secret, Sink, Source, Table};
use async_trait::async_trait;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataVersion(i64);

impl MetadataVersion {
    pub fn new(value: i64) -> Self {
        Self(value)
    }

    pub fn value(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetadataRecord<T> {
    value: T,
    version: MetadataVersion,
}

impl<T> MetadataRecord<T> {
    pub fn new(value: T, version: MetadataVersion) -> Self {
        Self { value, version }
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn version(&self) -> MetadataVersion {
        self.version
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MetadataPutCondition {
    #[default]
    Unconditional,
    NotExists,
    Version(MetadataVersion),
}

#[async_trait]
pub trait Metadata: Send + Sync {
    async fn get_database(&self, name: &str) -> Result<Option<MetadataRecord<Database>>>;

    async fn put_database(
        &self,
        database: Database,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion>;

    async fn delete_database(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()>;

    async fn list_databases(&self) -> Result<Vec<MetadataRecord<Database>>>;

    async fn get_schema(
        &self,
        database: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Schema>>>;

    async fn put_schema(
        &self,
        database: &str,
        schema: Schema,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion>;

    async fn delete_schema(
        &self,
        database: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()>;

    async fn list_schemas(&self, database: &str) -> Result<Vec<MetadataRecord<Schema>>>;

    async fn get_secret(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Secret>>>;

    async fn put_secret(
        &self,
        database: &str,
        schema: &str,
        secret: Secret,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion>;

    async fn delete_secret(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()>;

    async fn list_secrets(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<MetadataRecord<Secret>>>;

    async fn get_connection(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Connection>>>;

    async fn put_connection(
        &self,
        database: &str,
        schema: &str,
        connection: Connection,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion>;

    async fn delete_connection(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()>;

    async fn list_connections(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<MetadataRecord<Connection>>>;

    async fn get_source(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Source>>>;

    async fn put_source(
        &self,
        database: &str,
        schema: &str,
        source: Source,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion>;

    async fn delete_source(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()>;

    async fn list_sources(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<MetadataRecord<Source>>>;

    async fn get_sink(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Sink>>>;

    async fn put_sink(
        &self,
        database: &str,
        schema: &str,
        sink: Sink,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion>;

    async fn delete_sink(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()>;

    async fn list_sinks(&self, database: &str, schema: &str) -> Result<Vec<MetadataRecord<Sink>>>;

    async fn get_table(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Table>>>;

    async fn put_table(
        &self,
        database: &str,
        schema: &str,
        table: Table,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion>;

    async fn delete_table(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()>;

    async fn list_tables(&self, database: &str, schema: &str)
    -> Result<Vec<MetadataRecord<Table>>>;
}
