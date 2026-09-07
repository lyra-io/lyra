use crate::metadata::{
    Metadata, MetadataError, MetadataPutCondition, MetadataRecord, MetadataVersion, Result,
};
use crate::proto::pb_catalog::{Connection, Database, Schema, Secret, Sink, Source, Table, User};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use tokio::sync::RwLock;

type SchemaKey = (String, String);
type ObjectKey = (String, String, String);

#[derive(Default)]
struct State {
    // Mutable state
    next_version: i64,
    next_user_id: u32,
    users: HashMap<String, MetadataRecord<User>>,
    databases: HashMap<String, MetadataRecord<Database>>,
    schemas: HashMap<SchemaKey, MetadataRecord<Schema>>,
    secrets: HashMap<ObjectKey, MetadataRecord<Secret>>,
    connections: HashMap<ObjectKey, MetadataRecord<Connection>>,
    sources: HashMap<ObjectKey, MetadataRecord<Source>>,
    sinks: HashMap<ObjectKey, MetadataRecord<Sink>>,
    tables: HashMap<ObjectKey, MetadataRecord<Table>>,
}

#[derive(Default)]
pub struct MemoryMetadata {
    // Mutable state
    state: RwLock<State>,
}

impl MemoryMetadata {
    pub fn new() -> Self {
        Self::default()
    }
}

fn get0<K, T>(records: &HashMap<K, MetadataRecord<T>>, key: &K) -> Option<MetadataRecord<T>>
where
    K: Eq + Hash,
    T: Clone,
{
    records.get(key).cloned()
}

fn put0<K, T>(
    records: &mut HashMap<K, MetadataRecord<T>>,
    next_version: &mut i64,
    key: K,
    value: T,
    condition: MetadataPutCondition,
) -> Result<MetadataVersion>
where
    K: Clone + Debug + Eq + Hash,
{
    let valid = match condition {
        MetadataPutCondition::Unconditional => true,
        MetadataPutCondition::NotExists => !records.contains_key(&key),
        MetadataPutCondition::Version(version) => records
            .get(&key)
            .is_some_and(|record| record.version() == version),
    };
    if !valid {
        return Err(MetadataError::Conflict(format!("{key:?}")));
    }

    *next_version = next_version
        .checked_add(1)
        .ok_or_else(|| MetadataError::CounterExhausted("memory-version".to_string()))?;
    let version = MetadataVersion::new(*next_version);
    records.insert(key, MetadataRecord::new(value, version));
    Ok(version)
}

fn delete0<K, T>(
    records: &mut HashMap<K, MetadataRecord<T>>,
    key: &K,
    expected_version: Option<MetadataVersion>,
) -> Result<()>
where
    K: Debug + Eq + Hash,
{
    let valid = records
        .get(key)
        .is_some_and(|record| expected_version.is_none_or(|version| record.version() == version));
    if !valid {
        return Err(MetadataError::Conflict(format!("{key:?}")));
    }
    records.remove(key);
    Ok(())
}

fn list0<K, T>(
    records: &HashMap<K, MetadataRecord<T>>,
    include: impl Fn(&K) -> bool,
) -> Vec<MetadataRecord<T>>
where
    K: Eq + Hash,
    T: Clone,
{
    records
        .iter()
        .filter(|(key, _)| include(key))
        .map(|(_, record)| record.clone())
        .collect()
}

fn schema_key0(database: &str, name: &str) -> SchemaKey {
    (database.to_string(), name.to_string())
}

fn object_key0(database: &str, schema: &str, name: &str) -> ObjectKey {
    (database.to_string(), schema.to_string(), name.to_string())
}

#[async_trait]
impl Metadata for MemoryMetadata {
    async fn allocate_user_id(&self) -> Result<u32> {
        let mut state = self.state.write().await;
        state.next_user_id = state
            .next_user_id
            .checked_add(1)
            .ok_or_else(|| MetadataError::CounterExhausted("memory-user-id".to_string()))?;
        Ok(state.next_user_id)
    }

    async fn get_user(&self, name: &str) -> Result<Option<MetadataRecord<User>>> {
        Ok(get0(&self.state.read().await.users, &name.to_string()))
    }

    async fn put_user(
        &self,
        user: User,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let mut state = self.state.write().await;
        let State {
            next_version,
            users,
            ..
        } = &mut *state;
        put0(users, next_version, user.name.clone(), user, condition)
    }

    async fn delete_user(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        delete0(
            &mut self.state.write().await.users,
            &name.to_string(),
            expected_version,
        )
    }

    async fn list_users(&self) -> Result<Vec<MetadataRecord<User>>> {
        Ok(list0(&self.state.read().await.users, |_| true))
    }

    async fn rename_user(
        &self,
        name: &str,
        user: User,
        expected_version: MetadataVersion,
    ) -> Result<MetadataVersion> {
        let mut state = self.state.write().await;
        let old_name = name.to_string();
        if state.users.contains_key(&user.name)
            || !state
                .users
                .get(&old_name)
                .is_some_and(|record| record.version() == expected_version)
        {
            return Err(MetadataError::Conflict(old_name));
        }
        let State {
            next_version,
            users,
            ..
        } = &mut *state;
        let version = put0(
            users,
            next_version,
            user.name.clone(),
            user,
            MetadataPutCondition::NotExists,
        )?;
        users.remove(&old_name);
        Ok(version)
    }

    async fn delete_users(&self, users: &[(String, MetadataVersion)]) -> Result<()> {
        let mut state = self.state.write().await;
        if let Some((name, _)) = users.iter().find(|(name, version)| {
            !state
                .users
                .get(name)
                .is_some_and(|record| record.version() == *version)
        }) {
            return Err(MetadataError::Conflict(name.clone()));
        }
        for (name, _) in users {
            state.users.remove(name);
        }
        Ok(())
    }

    async fn get_database(&self, name: &str) -> Result<Option<MetadataRecord<Database>>> {
        Ok(get0(&self.state.read().await.databases, &name.to_string()))
    }

    async fn put_database(
        &self,
        database: Database,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let mut state = self.state.write().await;
        let State {
            next_version,
            databases,
            ..
        } = &mut *state;
        put0(
            databases,
            next_version,
            database.name.clone(),
            database,
            condition,
        )
    }

    async fn delete_database(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        delete0(
            &mut self.state.write().await.databases,
            &name.to_string(),
            expected_version,
        )
    }

    async fn list_databases(&self) -> Result<Vec<MetadataRecord<Database>>> {
        Ok(list0(&self.state.read().await.databases, |_| true))
    }

    async fn get_schema(
        &self,
        database: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Schema>>> {
        Ok(get0(
            &self.state.read().await.schemas,
            &schema_key0(database, name),
        ))
    }

    async fn put_schema(
        &self,
        database: &str,
        schema: Schema,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let mut state = self.state.write().await;
        let State {
            next_version,
            schemas,
            ..
        } = &mut *state;
        put0(
            schemas,
            next_version,
            schema_key0(database, &schema.name),
            schema,
            condition,
        )
    }

    async fn delete_schema(
        &self,
        database: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        delete0(
            &mut self.state.write().await.schemas,
            &schema_key0(database, name),
            expected_version,
        )
    }

    async fn list_schemas(&self, database: &str) -> Result<Vec<MetadataRecord<Schema>>> {
        Ok(list0(&self.state.read().await.schemas, |key| {
            key.0 == database
        }))
    }

    async fn get_secret(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Secret>>> {
        Ok(get0(
            &self.state.read().await.secrets,
            &object_key0(database, schema, name),
        ))
    }

    async fn put_secret(
        &self,
        database: &str,
        schema: &str,
        secret: Secret,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let mut state = self.state.write().await;
        let State {
            next_version,
            secrets,
            ..
        } = &mut *state;
        put0(
            secrets,
            next_version,
            object_key0(database, schema, &secret.name),
            secret,
            condition,
        )
    }

    async fn delete_secret(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        delete0(
            &mut self.state.write().await.secrets,
            &object_key0(database, schema, name),
            expected_version,
        )
    }

    async fn list_secrets(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<MetadataRecord<Secret>>> {
        Ok(list0(&self.state.read().await.secrets, |key| {
            key.0 == database && key.1 == schema
        }))
    }

    async fn get_connection(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Connection>>> {
        Ok(get0(
            &self.state.read().await.connections,
            &object_key0(database, schema, name),
        ))
    }

    async fn put_connection(
        &self,
        database: &str,
        schema: &str,
        connection: Connection,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let mut state = self.state.write().await;
        let State {
            next_version,
            connections,
            ..
        } = &mut *state;
        put0(
            connections,
            next_version,
            object_key0(database, schema, &connection.name),
            connection,
            condition,
        )
    }

    async fn delete_connection(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        delete0(
            &mut self.state.write().await.connections,
            &object_key0(database, schema, name),
            expected_version,
        )
    }

    async fn list_connections(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<MetadataRecord<Connection>>> {
        Ok(list0(&self.state.read().await.connections, |key| {
            key.0 == database && key.1 == schema
        }))
    }

    async fn get_source(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Source>>> {
        Ok(get0(
            &self.state.read().await.sources,
            &object_key0(database, schema, name),
        ))
    }

    async fn put_source(
        &self,
        database: &str,
        schema: &str,
        source: Source,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let mut state = self.state.write().await;
        let State {
            next_version,
            sources,
            ..
        } = &mut *state;
        put0(
            sources,
            next_version,
            object_key0(database, schema, &source.name),
            source,
            condition,
        )
    }

    async fn delete_source(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        delete0(
            &mut self.state.write().await.sources,
            &object_key0(database, schema, name),
            expected_version,
        )
    }

    async fn list_sources(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<MetadataRecord<Source>>> {
        Ok(list0(&self.state.read().await.sources, |key| {
            key.0 == database && key.1 == schema
        }))
    }

    async fn get_sink(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Sink>>> {
        Ok(get0(
            &self.state.read().await.sinks,
            &object_key0(database, schema, name),
        ))
    }

    async fn put_sink(
        &self,
        database: &str,
        schema: &str,
        sink: Sink,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let mut state = self.state.write().await;
        let State {
            next_version,
            sinks,
            ..
        } = &mut *state;
        put0(
            sinks,
            next_version,
            object_key0(database, schema, &sink.name),
            sink,
            condition,
        )
    }

    async fn delete_sink(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        delete0(
            &mut self.state.write().await.sinks,
            &object_key0(database, schema, name),
            expected_version,
        )
    }

    async fn list_sinks(&self, database: &str, schema: &str) -> Result<Vec<MetadataRecord<Sink>>> {
        Ok(list0(&self.state.read().await.sinks, |key| {
            key.0 == database && key.1 == schema
        }))
    }

    async fn get_table(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Table>>> {
        Ok(get0(
            &self.state.read().await.tables,
            &object_key0(database, schema, name),
        ))
    }

    async fn put_table(
        &self,
        database: &str,
        schema: &str,
        table: Table,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        let mut state = self.state.write().await;
        let State {
            next_version,
            tables,
            ..
        } = &mut *state;
        put0(
            tables,
            next_version,
            object_key0(database, schema, &table.name),
            table,
            condition,
        )
    }

    async fn delete_table(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        delete0(
            &mut self.state.write().await.tables,
            &object_key0(database, schema, name),
            expected_version,
        )
    }

    async fn list_tables(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<MetadataRecord<Table>>> {
        Ok(list0(&self.state.read().await.tables, |key| {
            key.0 == database && key.1 == schema
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(name: &str, id: u32) -> User {
        User {
            name: name.to_string(),
            id,
            ..User::default()
        }
    }

    #[tokio::test]
    async fn atomically_renames_and_deletes_users() {
        let metadata = MemoryMetadata::new();
        let alice_version = metadata
            .put_user(user("alice", 1), MetadataPutCondition::NotExists)
            .await
            .unwrap();
        let bob_version = metadata
            .put_user(user("bob", 2), MetadataPutCondition::NotExists)
            .await
            .unwrap();

        let admin_version = metadata
            .rename_user("alice", user("admin", 1), alice_version)
            .await
            .unwrap();
        metadata
            .delete_users(&[
                ("admin".to_string(), admin_version),
                ("bob".to_string(), bob_version),
            ])
            .await
            .unwrap();

        assert!(metadata.list_users().await.unwrap().is_empty());
    }
}
