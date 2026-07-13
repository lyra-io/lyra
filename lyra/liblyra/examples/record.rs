use liblyra::lyra::{Lyra, LyraOptions};
use liblyra::{Event, StreamOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = catalog::build_catalog(&catalog::CatalogOptions::default()).await?;
    let lyra = Lyra::new(catalog, LyraOptions::new());
    let stream = lyra
        .open_stream("record-example", StreamOptions::new().replication_factor(1))
        .await?;

    // Single record — blocks until durably acked
    let offset = stream.record(Event::new(b"hello world".to_vec())).await?;
    println!("single record at offset: {}", offset.0);

    // Record with key (for compaction)
    let offset = stream
        .record(Event::new(b"user updated".to_vec()).with_key(b"user-42".to_vec()))
        .await?;
    println!("keyed record at offset: {}", offset.0);

    // Record with transaction id
    let offset = stream
        .record(Event::new(b"txn event".to_vec()).with_txn_id(999))
        .await?;
    println!("txn record at offset: {}", offset.0);

    stream.close().await;
    Ok(())
}
