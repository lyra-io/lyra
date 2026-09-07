use meta::metadata::oxia::{OxiaMetadata, OxiaOptions};
use meta::metadata::{Metadata, MetadataPutCondition};
use meta::proto::pb_catalog::{
    Column, Connection, Database, PasswordCredential, Schema, Secret, SecretRef, Sink, Source,
    Table, User,
};
use std::collections::HashMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
#[ignore = "requires an Oxia service configured by OXIA_SERVICE_ADDRESS"]
async fn stores_typed_protobuf_metadata() {
    let address = env::var("OXIA_SERVICE_ADDRESS").unwrap();
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let database_name = format!("test-database-{suffix}");
    let user_name = format!("test-user-{suffix}");
    let schema_name = "public";
    let secret_name = format!("test-secret-{suffix}");
    let connection_name = format!("test-connection-{suffix}");
    let source_name = format!("test-source-{suffix}");
    let sink_name = format!("test-sink-{suffix}");
    let table_name = format!("test-table-{suffix}");
    let renamed_user_name = format!("test-renamed-user-{suffix}");
    let metadata = OxiaMetadata::new(&OxiaOptions::new(address, "default"))
        .await
        .unwrap();
    let user_id = metadata.allocate_user_id().await.unwrap();

    let user_version = metadata
        .put_user(
            User {
                name: user_name.clone(),
                is_superuser: false,
                can_create_database: true,
                can_create_user: false,
                password: Some(PasswordCredential {
                    salt: b"salt".to_vec().into(),
                    salted_password: b"hash".to_vec().into(),
                    iterations: 4096,
                }),
                id: user_id,
            },
            MetadataPutCondition::NotExists,
        )
        .await
        .unwrap();
    let database_version = metadata
        .put_database(
            Database {
                name: database_name.clone(),
            },
            MetadataPutCondition::NotExists,
        )
        .await
        .unwrap();
    let schema_version = metadata
        .put_schema(
            &database_name,
            Schema {
                name: schema_name.to_string(),
            },
            MetadataPutCondition::NotExists,
        )
        .await
        .unwrap();
    let secret_version = metadata
        .put_secret(
            &database_name,
            schema_name,
            Secret {
                name: secret_name.clone(),
                value: b"password".to_vec().into(),
            },
            MetadataPutCondition::NotExists,
        )
        .await
        .unwrap();
    let connection_version = metadata
        .put_connection(
            &database_name,
            schema_name,
            Connection {
                name: connection_name.clone(),
                options: HashMap::from([
                    ("type".to_string(), "kafka".to_string()),
                    ("brokers".to_string(), "localhost:9092".to_string()),
                ]),
                secret_refs: vec![SecretRef {
                    name: "sasl_password".to_string(),
                    data: secret_name.clone(),
                }],
            },
            MetadataPutCondition::NotExists,
        )
        .await
        .unwrap();
    let source_version = metadata
        .put_source(
            &database_name,
            schema_name,
            Source {
                name: source_name.clone(),
                options: HashMap::from([
                    ("connection".to_string(), connection_name.clone()),
                    ("topic".to_string(), "orders".to_string()),
                ]),
            },
            MetadataPutCondition::NotExists,
        )
        .await
        .unwrap();
    let sink_version = metadata
        .put_sink(
            &database_name,
            schema_name,
            Sink {
                name: sink_name.clone(),
                options: HashMap::from([
                    ("connection".to_string(), connection_name.clone()),
                    ("topic".to_string(), "orders-output".to_string()),
                ]),
            },
            MetadataPutCondition::NotExists,
        )
        .await
        .unwrap();
    let table_version = metadata
        .put_table(
            &database_name,
            schema_name,
            Table {
                name: table_name.clone(),
                source: source_name.clone(),
                sink: sink_name.clone(),
                columns: vec![Column {
                    name: "id".to_string(),
                    data_type: "BIGINT".to_string(),
                    nullable: false,
                }],
                primary_key: vec!["id".to_string()],
                options: HashMap::new(),
            },
            MetadataPutCondition::NotExists,
        )
        .await
        .unwrap();

    let stored = metadata
        .get_connection(&database_name, schema_name, &connection_name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.value().name, connection_name);
    assert_eq!(stored.value().options["type"], "kafka");
    assert_eq!(stored.value().secret_refs[0].data, secret_name);
    assert!(
        metadata
            .get_user(&user_name)
            .await
            .unwrap()
            .unwrap()
            .value()
            .can_create_database
    );
    assert_eq!(
        metadata
            .get_table(&database_name, schema_name, &table_name)
            .await
            .unwrap()
            .unwrap()
            .value()
            .source,
        source_name
    );

    metadata
        .delete_table(
            &database_name,
            schema_name,
            &table_name,
            Some(table_version),
        )
        .await
        .unwrap();
    metadata
        .delete_sink(&database_name, schema_name, &sink_name, Some(sink_version))
        .await
        .unwrap();
    metadata
        .delete_source(
            &database_name,
            schema_name,
            &source_name,
            Some(source_version),
        )
        .await
        .unwrap();
    metadata
        .delete_connection(
            &database_name,
            schema_name,
            &connection_name,
            Some(connection_version),
        )
        .await
        .unwrap();
    metadata
        .delete_secret(
            &database_name,
            schema_name,
            &secret_name,
            Some(secret_version),
        )
        .await
        .unwrap();
    metadata
        .delete_schema(&database_name, schema_name, Some(schema_version))
        .await
        .unwrap();
    metadata
        .delete_database(&database_name, Some(database_version))
        .await
        .unwrap();
    let mut user = metadata
        .get_user(&user_name)
        .await
        .unwrap()
        .unwrap()
        .value()
        .clone();
    user.name = renamed_user_name.clone();
    let user_version = metadata
        .rename_user(&user_name, user, user_version)
        .await
        .unwrap();
    metadata
        .delete_users(&[(renamed_user_name, user_version)])
        .await
        .unwrap();
}
