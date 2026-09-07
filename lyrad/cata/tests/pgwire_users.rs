use cata::Cata;
use cata::options::CataOptions;
use meta::metadata::MemoryMetadata;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::{Client, NoTls};

#[tokio::test]
async fn authenticates_catalog_users() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let metadata = Arc::new(MemoryMetadata::new());
    let options = CataOptions::new("127.0.0.1", port).with_bootstrap_user("root", "s3cr3t");
    let cata = Cata::new(options, metadata).await.unwrap();
    let server = tokio::spawn(cata.serve());

    let root = connect0(port, "root", "s3cr3t").await;
    root.batch_execute("CREATE USER reader WITH PASSWORD 'reader-password'")
        .await
        .unwrap();
    root.batch_execute("CREATE SECRET login_password VALUE 'secret-password'")
        .await
        .unwrap();
    root.batch_execute("CREATE USER secret_reader PASSWORD SECRET login_password")
        .await
        .unwrap();

    let reader = connect0(port, "reader", "reader-password").await;
    let value: i64 = reader.query_one("SELECT 1", &[]).await.unwrap().get(0);
    assert_eq!(value, 1);
    let secret_reader = connect0(port, "secret_reader", "secret-password").await;
    let value: i64 = secret_reader
        .query_one("SELECT 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(value, 1);

    root.batch_execute("ALTER USER reader PASSWORD SECRET login_password")
        .await
        .unwrap();
    root.batch_execute("DROP SECRET login_password")
        .await
        .unwrap();
    let reader = connect0(port, "reader", "secret-password").await;
    let value: i64 = reader.query_one("SELECT 1", &[]).await.unwrap().get(0);
    assert_eq!(value, 1);

    root.batch_execute("ALTER USER secret_reader PASSWORD NULL")
        .await
        .unwrap();
    let without_password = format!(
        "host=127.0.0.1 port={port} user=secret_reader password=secret-password \
         dbname=dev sslmode=disable"
    );
    assert!(
        tokio_postgres::connect(&without_password, NoTls)
            .await
            .is_err()
    );

    let rows = root
        .query("SELECT name FROM rw_catalog.rw_users ORDER BY name", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, String>(0), "reader");
    assert_eq!(rows[1].get::<_, String>(0), "root");
    assert_eq!(rows[2].get::<_, String>(0), "secret_reader");

    let rows = root.query("SHOW USERS", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, String>(0), "reader");
    assert_eq!(rows[1].get::<_, String>(0), "root");
    assert_eq!(rows[2].get::<_, String>(0), "secret_reader");

    let roles = root
        .query(
            "SELECT rolname FROM pg_catalog.pg_roles ORDER BY rolname",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(roles.len(), 3);

    let invalid =
        format!("host=127.0.0.1 port={port} user=root password=wrong dbname=dev sslmode=disable");
    assert!(tokio_postgres::connect(&invalid, NoTls).await.is_err());

    server.abort();
}

async fn connect0(port: u16, user: &str, password: &str) -> Client {
    let connection = format!(
        "host=127.0.0.1 port={port} user={user} password={password} dbname=dev sslmode=disable"
    );
    let mut last_error = None;
    for _ in 0..50 {
        match tokio_postgres::connect(&connection, NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                return client;
            }
            Err(error) => last_error = Some(error),
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("failed to connect to Cata: {}", last_error.unwrap());
}
