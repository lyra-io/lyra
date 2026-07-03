use libchronicle::chronicle::{Chronicle, ChronicleOptions};
use libchronicle::{Event, TimelineOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog =
        chronicle_catalog::build_catalog(&chronicle_catalog::CatalogOptions::default()).await?;
    let chronicle = Chronicle::new(catalog, ChronicleOptions::new());
    let timeline = chronicle
        .open_timeline(
            "record-example",
            TimelineOptions::new().replication_factor(1),
        )
        .await?;

    // Single record — blocks until durably acked
    let offset = timeline.record(Event::new(b"hello world".to_vec())).await?;
    println!("single record at offset: {}", offset.0);

    // Record with key (for compaction)
    let offset = timeline
        .record(Event::new(b"user updated".to_vec()).with_key(b"user-42".to_vec()))
        .await?;
    println!("keyed record at offset: {}", offset.0);

    // Record with transaction id
    let offset = timeline
        .record(Event::new(b"txn event".to_vec()).with_txn_id(999))
        .await?;
    println!("txn record at offset: {}", offset.0);

    timeline.close().await;
    Ok(())
}
