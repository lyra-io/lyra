use liblyra::lyra::{Lyra, LyraOptions};
use liblyra::{Event, StreamOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = catalog::build_catalog(&catalog::CatalogOptions::default()).await?;

    let lyra = Lyra::new(catalog, LyraOptions::new());

    let stream = lyra
        .open_stream("example-stream", StreamOptions::new().replication_factor(1))
        .await?;

    // Append events
    let o1 = stream.append(Event::new(b"hello".to_vec())).await?;
    let o2 = stream.append(Event::new(b"world".to_vec())).await?;
    println!("appended at offsets: {}, {}", o1.0, o2.0);

    // Append with key
    let o3 = stream
        .append(Event::new(b"keyed event".to_vec()).with_key(b"user-123".to_vec()))
        .await?;
    println!("appended keyed event at offset: {}", o3.0);

    // Batch appends with tokio::join!
    let (r1, r2, r3) = tokio::join!(
        stream.append(Event::new(b"batch-1".to_vec())),
        stream.append(Event::new(b"batch-2".to_vec())),
        stream.append(Event::new(b"batch-3".to_vec())),
    );
    println!("batch offsets: {}, {}, {}", r1?.0, r2?.0, r3?.0);

    // TODO: read will be reimplemented with catalog-based reader

    stream.close().await;

    Ok(())
}
