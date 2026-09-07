use meta::metadata::oxia::{OxiaMetadata, OxiaOptions};
use meta::metadata::{Metadata, MetadataPutCondition};
use meta::proto::pb_catalog::{
    Connection, Database, PasswordCredential, Schema, Secret, SecretRef, User,
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
    let renamed_user_name = format!("test-renamed-user-{suffix}");
    let metadata = OxiaMetadata::new(&OxiaOptions::new(address, "default"))
        .await
        .unwrap();
    let user_version = metadata
        .put_user(
            User {
                name: user_name.clone(),
                password: Some(PasswordCredential {
                    salt: b"salt".to_vec().into(),
                    salted_password: b"hash".to_vec().into(),
                    iterations: 4096,
                }),
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
    let stored = metadata
        .get_connection(&database_name, schema_name, &connection_name)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.value().name, connection_name);
    assert_eq!(stored.value().options["type"], "kafka");
    assert_eq!(stored.value().secret_refs[0].data, secret_name);
    assert_eq!(
        metadata
            .get_user(&user_name)
            .await
            .unwrap()
            .unwrap()
            .value()
            .name,
        user_name
    );
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
