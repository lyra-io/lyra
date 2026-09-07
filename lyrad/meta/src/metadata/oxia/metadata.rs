use crate::metadata::oxia::OxiaOptions;
use crate::metadata::oxia::keyspace::Keyspace;
use crate::metadata::path::{
    CONNECTION_PATH, DATABASE_PATH, SCHEMA_PATH, SECRET_PATH, SINK_PATH, SOURCE_PATH, TABLE_PATH,
    USER_ID_PATH, USER_PARTITION_KEY, USER_PATH,
};
use crate::metadata::{
    Metadata, MetadataError, MetadataPutCondition, MetadataRecord, MetadataVersion, Result,
};
use crate::proto::pb_catalog::{
    CatalogCounter, Connection, Database, Schema, Secret, Sink, Source, Table, User,
};
use async_trait::async_trait;
use futures_util::future::try_join_all;
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

    fn schema_path0(&self, database: &str) -> Result<String> {
        self.keyspace
            .collection(DATABASE_PATH, database, SCHEMA_PATH)
    }

    fn object_path0(&self, database: &str, schema: &str, path: &str) -> Result<String> {
        self.keyspace
            .collection(&self.schema_path0(database)?, schema, path)
    }

    async fn get0<T>(
        &self,
        key: &str,
        partition_key: Option<&str>,
    ) -> Result<Option<MetadataRecord<T>>>
    where
        T: Message + Default,
    {
        let request = self.client.get(key);
        let result = match partition_key {
            Some(partition_key) => request.partition_key(partition_key).await,
            None => request.await,
        };
        match result {
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
        partition_key: Option<&str>,
    ) -> Result<MetadataVersion>
    where
        T: Message,
    {
        let request = self.client.put(key, value.encode_to_vec());
        let request = match partition_key {
            Some(partition_key) => request.partition_key(partition_key),
            None => request,
        };
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

    async fn delete0(
        &self,
        key: &str,
        expected_version: Option<MetadataVersion>,
        partition_key: Option<&str>,
    ) -> Result<()> {
        let request = self.client.delete(key);
        let request = match partition_key {
            Some(partition_key) => request.partition_key(partition_key),
            None => request,
        };
        let result = match expected_version {
            Some(version) => request.expected_version_id(version.value()).await,
            None => request.await,
        };
        result.map_err(|error| Self::write_error(key, error))?;
        Ok(())
    }

    async fn scan0<T>(
        &self,
        prefix: &str,
        partition_key: Option<&str>,
    ) -> Result<Vec<MetadataRecord<T>>>
    where
        T: Message + Default,
    {
        let (first, last) = self.keyspace.range(prefix)?;
        let request = self.client.range_scan(first, last);
        let records = match partition_key {
            Some(partition_key) => request.partition_key(partition_key).await?,
            None => request.await?,
        };
        records
            .into_iter()
            .map(|record| {
                let value = T::decode(record.value.unwrap_or_default())?;
                let version = MetadataVersion::new(record.version.version_id);
                Ok(MetadataRecord::new(value, version))
            })
            .collect()
    }

    async fn scan_direct0<T>(
        &self,
        prefix: &str,
        partition_key: Option<&str>,
    ) -> Result<Vec<MetadataRecord<T>>>
    where
        T: Message + Default,
    {
        let (first, last) = self.keyspace.range(prefix)?;
        let request = self.client.range_scan(first, last);
        let records = match partition_key {
            Some(partition_key) => request.partition_key(partition_key).await?,
            None => request.await?,
        };
        records
            .into_iter()
            .filter(|record| {
                record
                    .key
                    .strip_prefix(prefix)
                    .is_some_and(|name| !name.contains('/'))
            })
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
    async fn allocate_user_id(&self) -> Result<u32> {
        loop {
            let record = self
                .get0::<CatalogCounter>(USER_ID_PATH, Some(USER_PARTITION_KEY))
                .await?;
            let (value, condition) =
                match record {
                    Some(record) => (
                        record.value().value.checked_add(1).ok_or_else(|| {
                            MetadataError::CounterExhausted(USER_ID_PATH.to_string())
                        })?,
                        MetadataPutCondition::Version(record.version()),
                    ),
                    None => (1, MetadataPutCondition::NotExists),
                };
            let counter = CatalogCounter { value };
            match self
                .put0(USER_ID_PATH, &counter, condition, Some(USER_PARTITION_KEY))
                .await
            {
                Ok(_) => {
                    return u32::try_from(value)
                        .map_err(|_| MetadataError::CounterExhausted(USER_ID_PATH.to_string()));
                }
                Err(MetadataError::Conflict(_)) => continue,
                Err(error) => return Err(error),
            }
        }
    }

    async fn get_user(&self, name: &str) -> Result<Option<MetadataRecord<User>>> {
        let key = self.keyspace.object(USER_PATH, name)?;
        self.get0(&key, Some(USER_PARTITION_KEY)).await
    }

    async fn put_user(
        &self,
        user: User,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let key = self.keyspace.object(USER_PATH, &user.name)?;
        self.put0(&key, &user, condition, Some(USER_PARTITION_KEY))
            .await
    }

    async fn delete_user(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        let key = self.keyspace.object(USER_PATH, name)?;
        self.delete0(&key, expected_version, Some(USER_PARTITION_KEY))
            .await
    }

    async fn list_users(&self) -> Result<Vec<MetadataRecord<User>>> {
        self.scan_direct0(USER_PATH, Some(USER_PARTITION_KEY)).await
    }

    async fn rename_user(
        &self,
        name: &str,
        user: User,
        expected_version: MetadataVersion,
    ) -> Result<MetadataVersion> {
        let old_key = self.keyspace.object(USER_PATH, name)?;
        let new_key = self.keyspace.object(USER_PATH, &user.name)?;
        let put = self.put0(
            &new_key,
            &user,
            MetadataPutCondition::NotExists,
            Some(USER_PARTITION_KEY),
        );
        let delete = self.delete0(&old_key, Some(expected_version), Some(USER_PARTITION_KEY));
        let (version, ()) = tokio::try_join!(put, delete)?;
        Ok(version)
    }

    async fn delete_users(&self, users: &[(String, MetadataVersion)]) -> Result<()> {
        let records = users
            .iter()
            .map(|(name, version)| Ok((self.keyspace.object(USER_PATH, name)?, *version)))
            .collect::<Result<Vec<_>>>()?;
        try_join_all(
            records
                .iter()
                .map(|(key, version)| self.delete0(key, Some(*version), Some(USER_PARTITION_KEY))),
        )
        .await?;
        Ok(())
    }

    async fn get_database(&self, name: &str) -> Result<Option<MetadataRecord<Database>>> {
        let key = self.keyspace.object(DATABASE_PATH, name)?;
        self.get0(&key, Some(name)).await
    }

    async fn put_database(
        &self,
        database: Database,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let key = self.keyspace.object(DATABASE_PATH, &database.name)?;
        self.put0(&key, &database, condition, Some(&database.name))
            .await
    }

    async fn delete_database(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        let key = self.keyspace.object(DATABASE_PATH, name)?;
        self.delete0(&key, expected_version, Some(name)).await
    }

    async fn list_databases(&self) -> Result<Vec<MetadataRecord<Database>>> {
        self.scan_direct0(DATABASE_PATH, None).await
    }

    async fn get_schema(
        &self,
        database: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Schema>>> {
        let key = self.keyspace.object(&self.schema_path0(database)?, name)?;
        self.get0(&key, Some(database)).await
    }

    async fn put_schema(
        &self,
        database: &str,
        schema: Schema,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let key = self
            .keyspace
            .object(&self.schema_path0(database)?, &schema.name)?;
        self.put0(&key, &schema, condition, Some(database)).await
    }

    async fn delete_schema(
        &self,
        database: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        let key = self.keyspace.object(&self.schema_path0(database)?, name)?;
        self.delete0(&key, expected_version, Some(database)).await
    }

    async fn list_schemas(&self, database: &str) -> Result<Vec<MetadataRecord<Schema>>> {
        self.scan_direct0(&self.schema_path0(database)?, Some(database))
            .await
    }

    async fn get_secret(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Secret>>> {
        let path = self.object_path0(database, schema, SECRET_PATH)?;
        let key = self.keyspace.object(&path, name)?;
        self.get0(&key, Some(database)).await
    }

    async fn put_secret(
        &self,
        database: &str,
        schema: &str,
        secret: Secret,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let path = self.object_path0(database, schema, SECRET_PATH)?;
        let key = self.keyspace.object(&path, &secret.name)?;
        self.put0(&key, &secret, condition, Some(database)).await
    }

    async fn delete_secret(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        let path = self.object_path0(database, schema, SECRET_PATH)?;
        let key = self.keyspace.object(&path, name)?;
        self.delete0(&key, expected_version, Some(database)).await
    }

    async fn list_secrets(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<MetadataRecord<Secret>>> {
        let path = self.object_path0(database, schema, SECRET_PATH)?;
        self.scan0(&path, Some(database)).await
    }

    async fn get_connection(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Connection>>> {
        let path = self.object_path0(database, schema, CONNECTION_PATH)?;
        let key = self.keyspace.object(&path, name)?;
        self.get0(&key, Some(database)).await
    }

    async fn put_connection(
        &self,
        database: &str,
        schema: &str,
        connection: Connection,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let path = self.object_path0(database, schema, CONNECTION_PATH)?;
        let key = self.keyspace.object(&path, &connection.name)?;
        self.put0(&key, &connection, condition, Some(database))
            .await
    }

    async fn delete_connection(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        let path = self.object_path0(database, schema, CONNECTION_PATH)?;
        let key = self.keyspace.object(&path, name)?;
        self.delete0(&key, expected_version, Some(database)).await
    }

    async fn list_connections(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<MetadataRecord<Connection>>> {
        let path = self.object_path0(database, schema, CONNECTION_PATH)?;
        self.scan0(&path, Some(database)).await
    }

    async fn get_source(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Source>>> {
        let path = self.object_path0(database, schema, SOURCE_PATH)?;
        let key = self.keyspace.object(&path, name)?;
        self.get0(&key, Some(database)).await
    }

    async fn put_source(
        &self,
        database: &str,
        schema: &str,
        source: Source,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let path = self.object_path0(database, schema, SOURCE_PATH)?;
        let key = self.keyspace.object(&path, &source.name)?;
        self.put0(&key, &source, condition, Some(database)).await
    }

    async fn delete_source(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        let path = self.object_path0(database, schema, SOURCE_PATH)?;
        let key = self.keyspace.object(&path, name)?;
        self.delete0(&key, expected_version, Some(database)).await
    }

    async fn list_sources(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<MetadataRecord<Source>>> {
        let path = self.object_path0(database, schema, SOURCE_PATH)?;
        self.scan0(&path, Some(database)).await
    }

    async fn get_sink(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Sink>>> {
        let path = self.object_path0(database, schema, SINK_PATH)?;
        let key = self.keyspace.object(&path, name)?;
        self.get0(&key, Some(database)).await
    }

    async fn put_sink(
        &self,
        database: &str,
        schema: &str,
        sink: Sink,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let path = self.object_path0(database, schema, SINK_PATH)?;
        let key = self.keyspace.object(&path, &sink.name)?;
        self.put0(&key, &sink, condition, Some(database)).await
    }

    async fn delete_sink(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        let path = self.object_path0(database, schema, SINK_PATH)?;
        let key = self.keyspace.object(&path, name)?;
        self.delete0(&key, expected_version, Some(database)).await
    }

    async fn list_sinks(&self, database: &str, schema: &str) -> Result<Vec<MetadataRecord<Sink>>> {
        let path = self.object_path0(database, schema, SINK_PATH)?;
        self.scan0(&path, Some(database)).await
    }

    async fn get_table(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Table>>> {
        let path = self.object_path0(database, schema, TABLE_PATH)?;
        let key = self.keyspace.object(&path, name)?;
        self.get0(&key, Some(database)).await
    }

    async fn put_table(
        &self,
        database: &str,
        schema: &str,
        table: Table,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let path = self.object_path0(database, schema, TABLE_PATH)?;
        let key = self.keyspace.object(&path, &table.name)?;
        self.put0(&key, &table, condition, Some(database)).await
    }

    async fn delete_table(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        let path = self.object_path0(database, schema, TABLE_PATH)?;
        let key = self.keyspace.object(&path, name)?;
        self.delete0(&key, expected_version, Some(database)).await
    }

    async fn list_tables(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<MetadataRecord<Table>>> {
        let path = self.object_path0(database, schema, TABLE_PATH)?;
        self.scan0(&path, Some(database)).await
    }
}
