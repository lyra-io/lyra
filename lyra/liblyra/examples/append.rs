use liblyra::lyra::{Lyra, LyraOptions};
use liblyra::{Event, StreamOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = catalog::build_catalog(&catalog::CatalogOptions::default()).await?;
    let lyra = Lyra::new(catalog, LyraOptions::new());
    let stream = lyra
        .open_stream("append-example", StreamOptions::new().replication_factor(1))
        .await?;

    // Single append: blocks until durably acked.
    let offset = stream.append(Event::new(b"hello world".to_vec())).await?;
    println!("single append at offset: {}", offset.0);

    // Append with key (for compaction)
    let offset = stream
        .append(Event::new(b"user updated".to_vec()).with_key(b"user-42".to_vec()))
        .await?;
    println!("keyed append at offset: {}", offset.0);

    // Append with transaction id
    let offset = stream
        .append(Event::new(b"txn event".to_vec()).with_txn_id(999))
        .await?;
    println!("txn append at offset: {}", offset.0);

    stream.close().await;
    Ok(())
}
