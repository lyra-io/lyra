use libchronicle::chronicle::{Chronicle, ChronicleOptions};
use libchronicle::{Event, TimelineOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog =
        chronicle_catalog::build_catalog(&chronicle_catalog::CatalogOptions::default()).await?;

    let chronicle = Chronicle::new(catalog, ChronicleOptions::new());

    let timeline = chronicle
        .open_timeline(
            "example-timeline",
            TimelineOptions::new().replication_factor(1),
        )
        .await?;

    // Record events
    let o1 = timeline.record(Event::new(b"hello".to_vec())).await?;
    let o2 = timeline.record(Event::new(b"world".to_vec())).await?;
    println!("recorded at offsets: {}, {}", o1.0, o2.0);

    // Record with key
    let o3 = timeline
        .record(Event::new(b"keyed event".to_vec()).with_key(b"user-123".to_vec()))
        .await?;
    println!("recorded keyed event at offset: {}", o3.0);

    // Batch records with tokio::join!
    let (r1, r2, r3) = tokio::join!(
        timeline.record(Event::new(b"batch-1".to_vec())),
        timeline.record(Event::new(b"batch-2".to_vec())),
        timeline.record(Event::new(b"batch-3".to_vec())),
    );
    println!("batch offsets: {}, {}, {}", r1?.0, r2?.0, r3?.0);

    // TODO: fetch will be reimplemented with catalog-based reader

    timeline.close().await;

    Ok(())
}
