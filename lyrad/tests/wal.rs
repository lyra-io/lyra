use bytes::Bytes;
use lyrad::wal::{IoMode, SegmentWal, Wal, WalOptions, WalReader};
use std::collections::BTreeSet;
use std::io::{Seek, Write};
use std::sync::Arc;
use std::time::Duration;

fn standard_options(path: &std::path::Path) -> WalOptions {
    let mut options = WalOptions::new(path);
    options.io_mode = IoMode::Standard;
    options.batch_linger = Duration::from_millis(1);
    options
}

#[tokio::test]
async fn appends_and_reads_opaque_payloads_by_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let wal = SegmentWal::open(standard_options(dir.path()))
        .await
        .unwrap();

    assert_eq!(
        wal.append(Bytes::from_static(b"one"), true).await.unwrap(),
        0
    );
    assert_eq!(wal.append(Bytes::new(), true).await.unwrap(), 1);
    assert_eq!(
        wal.append(Bytes::from_static(b"three"), true)
            .await
            .unwrap(),
        2
    );

    let mut reader = wal.new_reader(1).await.unwrap();
    assert_eq!(reader.read_next().await.unwrap(), Some((1, Bytes::new())));
    assert_eq!(
        reader.read_next().await.unwrap(),
        Some((2, Bytes::from_static(b"three")))
    );
    assert_eq!(reader.read_next().await.unwrap(), None);
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn reader_is_a_durable_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let wal = SegmentWal::open(standard_options(dir.path()))
        .await
        .unwrap();

    wal.append(Bytes::from_static(b"written"), false)
        .await
        .unwrap();
    let mut before_sync = wal.new_reader(0).await.unwrap();
    assert_eq!(before_sync.read_next().await.unwrap(), None);

    wal.append(Bytes::from_static(b"synced"), true)
        .await
        .unwrap();
    assert_eq!(before_sync.read_next().await.unwrap(), None);

    let mut after_sync = wal.new_reader(0).await.unwrap();
    assert_eq!(
        after_sync.read_next().await.unwrap(),
        Some((0, Bytes::from_static(b"written")))
    );
    assert_eq!(
        after_sync.read_next().await.unwrap(),
        Some((1, Bytes::from_static(b"synced")))
    );
    assert_eq!(after_sync.read_next().await.unwrap(), None);
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_recovers_records_and_continues_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let options = standard_options(dir.path());
    let wal = SegmentWal::open(options.clone()).await.unwrap();
    wal.append(Bytes::from_static(b"before"), true)
        .await
        .unwrap();
    wal.shutdown().await.unwrap();

    let wal = SegmentWal::open(options).await.unwrap();
    assert_eq!(
        wal.append(Bytes::from_static(b"after"), true)
            .await
            .unwrap(),
        1
    );
    let mut reader = wal.new_reader(0).await.unwrap();
    assert_eq!(
        reader.read_next().await.unwrap(),
        Some((0, Bytes::from_static(b"before")))
    );
    assert_eq!(
        reader.read_next().await.unwrap(),
        Some((1, Bytes::from_static(b"after")))
    );
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_discards_an_incomplete_final_record() {
    let dir = tempfile::tempdir().unwrap();
    let options = standard_options(dir.path());
    let wal = SegmentWal::open(options.clone()).await.unwrap();
    wal.append(Bytes::from(vec![0xAB; 16 * 1024]), true)
        .await
        .unwrap();
    wal.shutdown().await.unwrap();

    let path = std::fs::read_dir(dir.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(4096 + 64)
        .unwrap();

    let wal = SegmentWal::open(options).await.unwrap();
    let mut reader = wal.new_reader(0).await.unwrap();
    assert_eq!(reader.read_next().await.unwrap(), None);
    assert_eq!(
        wal.append(Bytes::from_static(b"replacement"), true)
            .await
            .unwrap(),
        0
    );
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn corruption_in_a_non_final_segment_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.max_segment_size = 8192;
    let wal = SegmentWal::open(options.clone()).await.unwrap();
    wal.append(Bytes::from(vec![0x11; 128]), true)
        .await
        .unwrap();
    wal.append(Bytes::from(vec![0x22; 128]), true)
        .await
        .unwrap();
    wal.shutdown().await.unwrap();

    let mut files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    files.sort();
    assert_eq!(files.len(), 2);
    let mut first = std::fs::OpenOptions::new()
        .write(true)
        .open(&files[0])
        .unwrap();
    first
        .seek(std::io::SeekFrom::Start(4096 + 11 + 12))
        .unwrap();
    first.write_all(&[0xFF]).unwrap();
    first.sync_data().unwrap();

    match SegmentWal::open(options).await {
        Ok(_) => panic!("corrupt non-final segment unexpectedly opened"),
        Err(error) => assert!(error.to_string().contains("checksum")),
    }
}

#[tokio::test]
async fn rotates_segment_files_without_changing_sequence_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.max_segment_size = 8192;
    let wal = SegmentWal::open(options).await.unwrap();

    for value in 0..4u8 {
        assert_eq!(
            wal.append(Bytes::from(vec![value; 128]), true)
                .await
                .unwrap(),
            value as u64
        );
    }

    let segment_count = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "seg"))
        .count();
    assert_eq!(segment_count, 4);

    let mut reader = wal.new_reader(0).await.unwrap();
    for value in 0..4u8 {
        assert_eq!(
            reader.read_next().await.unwrap(),
            Some((value as u64, Bytes::from(vec![value; 128])))
        );
    }
    wal.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_callers_receive_unique_ordered_sequences() {
    let dir = tempfile::tempdir().unwrap();
    let wal = SegmentWal::open(standard_options(dir.path()))
        .await
        .unwrap();
    let mut tasks = Vec::new();

    for value in 0..100u8 {
        let wal = Arc::clone(&wal);
        tasks.push(tokio::spawn(async move {
            wal.append(Bytes::from(vec![value]), true).await.unwrap()
        }));
    }

    let mut sequences = BTreeSet::new();
    for task in tasks {
        sequences.insert(task.await.unwrap());
    }
    assert_eq!(sequences, (0..100).collect());

    let mut reader = wal.new_reader(0).await.unwrap();
    for expected in 0..100 {
        let (sequence, _) = reader.read_next().await.unwrap().unwrap();
        assert_eq!(sequence, expected);
    }
    assert_eq!(reader.read_next().await.unwrap(), None);
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn direct_preferred_appends_and_reads_with_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.io_mode = IoMode::DirectPreferred;
    let wal = SegmentWal::open(options).await.unwrap();

    wal.append(Bytes::from_static(b"direct-or-standard"), true)
        .await
        .unwrap();
    let mut reader = wal.new_reader(0).await.unwrap();
    assert_eq!(
        reader.read_next().await.unwrap(),
        Some((0, Bytes::from_static(b"direct-or-standard")))
    );
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_is_idempotent_and_rejects_new_appends() {
    let dir = tempfile::tempdir().unwrap();
    let wal = SegmentWal::open(standard_options(dir.path()))
        .await
        .unwrap();
    wal.shutdown().await.unwrap();
    wal.shutdown().await.unwrap();
    assert!(wal.append(Bytes::from_static(b"late"), true).await.is_err());
}

#[tokio::test]
async fn oversized_payload_is_rejected_without_consuming_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.max_record_size = 3;
    let wal = SegmentWal::open(options).await.unwrap();

    assert!(wal.append(Bytes::from_static(b"four"), true).await.is_err());
    assert_eq!(
        wal.append(Bytes::from_static(b"ok"), true).await.unwrap(),
        0
    );
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_syncs_records_requested_as_written() {
    let dir = tempfile::tempdir().unwrap();
    let options = standard_options(dir.path());
    let wal = SegmentWal::open(options.clone()).await.unwrap();
    wal.append(Bytes::from_static(b"written"), false)
        .await
        .unwrap();
    wal.shutdown().await.unwrap();

    let wal = SegmentWal::open(options).await.unwrap();
    let mut reader = wal.new_reader(0).await.unwrap();
    assert_eq!(
        reader.read_next().await.unwrap(),
        Some((0, Bytes::from_static(b"written")))
    );
    wal.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_appends_and_shutdown_never_strands_a_caller() {
    let dir = tempfile::tempdir().unwrap();
    let wal = SegmentWal::open(standard_options(dir.path()))
        .await
        .unwrap();
    let mut appends = Vec::new();
    for value in 0..100u8 {
        let wal = Arc::clone(&wal);
        appends.push(tokio::spawn(async move {
            wal.append(Bytes::from(vec![value]), false).await
        }));
    }

    let shutdown = {
        let wal = Arc::clone(&wal);
        tokio::spawn(async move { wal.shutdown().await })
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        for append in appends {
            let _ = append.await.unwrap();
        }
        shutdown.await.unwrap().unwrap();
    })
    .await
    .expect("append/shutdown race timed out");
}
