use crate::metadata::oxia::OxiaOptions;
use crate::metadata::oxia::keyspace::Keyspace;
use crate::metadata::path::{CONNECTION_PATH, SECRET_PATH, SINK_PATH, SOURCE_PATH, TABLE_PATH};
use crate::metadata::{
    Metadata, MetadataError, MetadataPutCondition, MetadataRecord, MetadataVersion, Result,
};
use crate::proto::pb_catalog::{Connection, Secret, Sink, Source, Table};
use async_trait::async_trait;
use oxia::{OxiaClient, OxiaError};
use prost::Message;

pub struct OxiaMetadata {
    // Immutable state
    client: OxiaClient,
    keyspace: Keyspace,
}

impl OxiaMetadata {
    pub async fn new(options: &OxiaOptions) -> Result<Self> {
        let keyspace = Keyspace::new();
        let client = OxiaClient::builder()
            .service_address(options.service_address())
            .namespace(options.namespace())
            .build()
            .await?;

        Ok(Self { client, keyspace })
    }

    fn write_error(key: &str, error: OxiaError) -> MetadataError {
        match error {
            OxiaError::UnexpectedVersionId => MetadataError::Conflict(key.to_string()),
            error => MetadataError::Oxia(error),
        }
    }

    async fn get0<T>(&self, key: &str) -> Result<Option<MetadataRecord<T>>>
    where
        T: Message + Default,
    {
        match self.client.get(key).await {
            Ok(record) => {
                let value = T::decode(record.value.unwrap_or_default())?;
                let version = MetadataVersion::new(record.version.version_id);
                Ok(Some(MetadataRecord::new(value, version)))
            }
            Err(OxiaError::KeyNotFound) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn put0<T>(
        &self,
        key: &str,
        value: &T,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion>
    where
        T: Message,
    {
        let request = self.client.put(key, value.encode_to_vec());
        let result = match condition {
            MetadataPutCondition::Unconditional => request.await,
            MetadataPutCondition::NotExists => request.expected_record_not_exists().await,
            MetadataPutCondition::Version(version) => {
                request.expected_version_id(version.value()).await
            }
        }
        .map_err(|error| Self::write_error(key, error))?;

        Ok(MetadataVersion::new(result.version.version_id))
    }

    async fn delete0(&self, key: &str, expected_version: Option<MetadataVersion>) -> Result<()> {
        let request = self.client.delete(key);
        let result = match expected_version {
            Some(version) => request.expected_version_id(version.value()).await,
            None => request.await,
        };
        result.map_err(|error| Self::write_error(key, error))?;
        Ok(())
    }

    async fn scan0<T>(&self, prefix: &str) -> Result<Vec<MetadataRecord<T>>>
    where
        T: Message + Default,
    {
        let (first, last) = self.keyspace.range(prefix)?;
        self.client
            .range_scan(first, last)
            .await?
            .into_iter()
            .map(|record| {
                let value = T::decode(record.value.unwrap_or_default())?;
                let version = MetadataVersion::new(record.version.version_id);
                Ok(MetadataRecord::new(value, version))
            })
            .collect()
    }
}

#[async_trait]
impl Metadata for OxiaMetadata {
    async fn get_secret(&self, name: &str) -> Result<Option<MetadataRecord<Secret>>> {
        let key = self.keyspace.object(SECRET_PATH, name)?;
        self.get0(&key).await
    }

    async fn put_secret(
        &self,
        secret: Secret,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let key = self.keyspace.object(SECRET_PATH, &secret.name)?;
        self.put0(&key, &secret, condition).await
    }

    async fn delete_secret(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        let key = self.keyspace.object(SECRET_PATH, name)?;
        self.delete0(&key, expected_version).await
    }

    async fn list_secrets(&self) -> Result<Vec<MetadataRecord<Secret>>> {
        self.scan0(SECRET_PATH).await
    }

    async fn get_connection(&self, name: &str) -> Result<Option<MetadataRecord<Connection>>> {
        let key = self.keyspace.object(CONNECTION_PATH, name)?;
        self.get0(&key).await
    }

    async fn put_connection(
        &self,
        connection: Connection,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let key = self.keyspace.object(CONNECTION_PATH, &connection.name)?;
        self.put0(&key, &connection, condition).await
    }

    async fn delete_connection(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        let key = self.keyspace.object(CONNECTION_PATH, name)?;
        self.delete0(&key, expected_version).await
    }

    async fn list_connections(&self) -> Result<Vec<MetadataRecord<Connection>>> {
        self.scan0(CONNECTION_PATH).await
    }

    async fn get_source(&self, name: &str) -> Result<Option<MetadataRecord<Source>>> {
        let key = self.keyspace.object(SOURCE_PATH, name)?;
        self.get0(&key).await
    }

    async fn put_source(
        &self,
        source: Source,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let key = self.keyspace.object(SOURCE_PATH, &source.name)?;
        self.put0(&key, &source, condition).await
    }

    async fn delete_source(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        let key = self.keyspace.object(SOURCE_PATH, name)?;
        self.delete0(&key, expected_version).await
    }

    async fn list_sources(&self) -> Result<Vec<MetadataRecord<Source>>> {
        self.scan0(SOURCE_PATH).await
    }

    async fn get_sink(&self, name: &str) -> Result<Option<MetadataRecord<Sink>>> {
        let key = self.keyspace.object(SINK_PATH, name)?;
        self.get0(&key).await
    }

    async fn put_sink(
        &self,
        sink: Sink,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let key = self.keyspace.object(SINK_PATH, &sink.name)?;
        self.put0(&key, &sink, condition).await
    }

    async fn delete_sink(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        let key = self.keyspace.object(SINK_PATH, name)?;
        self.delete0(&key, expected_version).await
    }

    async fn list_sinks(&self) -> Result<Vec<MetadataRecord<Sink>>> {
        self.scan0(SINK_PATH).await
    }

    async fn get_table(&self, name: &str) -> Result<Option<MetadataRecord<Table>>> {
        let key = self.keyspace.object(TABLE_PATH, name)?;
        self.get0(&key).await
    }

    async fn put_table(
        &self,
        table: Table,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let key = self.keyspace.object(TABLE_PATH, &table.name)?;
        self.put0(&key, &table, condition).await
    }

    async fn delete_table(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        let key = self.keyspace.object(TABLE_PATH, name)?;
        self.delete0(&key, expected_version).await
    }

    async fn list_tables(&self) -> Result<Vec<MetadataRecord<Table>>> {
        self.scan0(TABLE_PATH).await
    }
}
