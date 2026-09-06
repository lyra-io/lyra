use crate::metadata::Result;
use crate::proto::pb_catalog::{Connection, Secret, Sink, Source, Table};
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
    async fn get_secret(&self, name: &str) -> Result<Option<MetadataRecord<Secret>>>;

    async fn put_secret(
        &self,
        secret: Secret,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion>;

    async fn delete_secret(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()>;

    async fn list_secrets(&self) -> Result<Vec<MetadataRecord<Secret>>>;

    async fn get_connection(&self, name: &str) -> Result<Option<MetadataRecord<Connection>>>;

    async fn put_connection(
        &self,
        connection: Connection,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion>;

    async fn delete_connection(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()>;

    async fn list_connections(&self) -> Result<Vec<MetadataRecord<Connection>>>;

    async fn get_source(&self, name: &str) -> Result<Option<MetadataRecord<Source>>>;

    async fn put_source(
        &self,
        source: Source,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion>;

    async fn delete_source(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()>;

    async fn list_sources(&self) -> Result<Vec<MetadataRecord<Source>>>;

    async fn get_sink(&self, name: &str) -> Result<Option<MetadataRecord<Sink>>>;

    async fn put_sink(
        &self,
        sink: Sink,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion>;

    async fn delete_sink(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()>;

    async fn list_sinks(&self) -> Result<Vec<MetadataRecord<Sink>>>;

    async fn get_table(&self, name: &str) -> Result<Option<MetadataRecord<Table>>>;

    async fn put_table(
        &self,
        table: Table,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion>;

    async fn delete_table(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()>;

    async fn list_tables(&self) -> Result<Vec<MetadataRecord<Table>>>;
}
