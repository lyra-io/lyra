use bytes::Bytes;
use lyrad::wal::{IoMode, SegmentWal, Wal, WalError, WalOptions};
use std::collections::BTreeSet;
use std::io::{Seek, Write};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

fn standard_options(path: &std::path::Path) -> WalOptions {
    let mut options = WalOptions::new(path);
    options.io_mode = IoMode::Standard;
    options.batch_linger = Duration::from_millis(1);
    options
}

async fn open_wal(options: WalOptions) -> Result<Arc<SegmentWal>, WalError> {
    let (_trim_tx, trim_rx) = watch::channel(None);
    SegmentWal::open(options, trim_rx).await
}

fn segment_count(path: &std::path::Path) -> usize {
    std::fs::read_dir(path)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "seg"))
        .count()
}

async fn wait_for_segment_count(path: &std::path::Path, expected: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if segment_count(path) == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "timed out waiting for {expected} WAL segments; found {}",
            segment_count(path)
        )
    });
}

#[tokio::test]
async fn appends_and_recovers_opaque_payloads_by_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(standard_options(dir.path())).await.unwrap();

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

    let mut recovery = wal.recover(1).unwrap();
    assert_eq!(
        recovery.next().transpose().unwrap(),
        Some((1, Bytes::new()))
    );
    assert_eq!(
        recovery.next().transpose().unwrap(),
        Some((2, Bytes::from_static(b"three")))
    );
    assert_eq!(recovery.next().transpose().unwrap(), None);
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovery_is_a_durable_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(standard_options(dir.path())).await.unwrap();

    wal.append(Bytes::from_static(b"written"), false)
        .await
        .unwrap();
    let mut before_sync = wal.recover(0).unwrap();
    assert_eq!(before_sync.next().transpose().unwrap(), None);

    wal.append(Bytes::from_static(b"synced"), true)
        .await
        .unwrap();
    assert_eq!(before_sync.next().transpose().unwrap(), None);

    let mut after_sync = wal.recover(0).unwrap();
    assert_eq!(
        after_sync.next().transpose().unwrap(),
        Some((0, Bytes::from_static(b"written")))
    );
    assert_eq!(
        after_sync.next().transpose().unwrap(),
        Some((1, Bytes::from_static(b"synced")))
    );
    assert_eq!(after_sync.next().transpose().unwrap(), None);
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_recovers_records_and_continues_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let options = standard_options(dir.path());
    let wal = open_wal(options.clone()).await.unwrap();
    wal.append(Bytes::from_static(b"before"), true)
        .await
        .unwrap();
    wal.shutdown().await.unwrap();

    let wal = open_wal(options).await.unwrap();
    assert_eq!(
        wal.append(Bytes::from_static(b"after"), true)
            .await
            .unwrap(),
        1
    );
    let mut recovery = wal.recover(0).unwrap();
    assert_eq!(
        recovery.next().transpose().unwrap(),
        Some((0, Bytes::from_static(b"before")))
    );
    assert_eq!(
        recovery.next().transpose().unwrap(),
        Some((1, Bytes::from_static(b"after")))
    );
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_discards_an_incomplete_final_record() {
    let dir = tempfile::tempdir().unwrap();
    let options = standard_options(dir.path());
    let wal = open_wal(options.clone()).await.unwrap();
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

    let wal = open_wal(options).await.unwrap();
    let mut recovery = wal.recover(0).unwrap();
    assert_eq!(recovery.next().transpose().unwrap(), None);
    assert_eq!(
        wal.append(Bytes::from_static(b"replacement"), true)
            .await
            .unwrap(),
        0
    );
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_discards_a_torn_tail_segment_header() {
    let dir = tempfile::tempdir().unwrap();
    let options = standard_options(dir.path());
    let wal = open_wal(options.clone()).await.unwrap();
    wal.append(Bytes::from_static(b"committed"), true)
        .await
        .unwrap();
    wal.shutdown().await.unwrap();
    assert_eq!(segment_count(dir.path()), 1);

    // Simulate a crash that left a never-synced tail segment with a torn header.
    let torn = dir.path().join("0000000002.seg");
    std::fs::write(&torn, b"LYRASEG\0").unwrap();

    let wal = open_wal(options).await.unwrap();
    assert!(
        !torn.exists(),
        "torn tail segment header should be discarded on recovery"
    );
    assert_eq!(
        wal.append(Bytes::from_static(b"after"), true)
            .await
            .unwrap(),
        1
    );
    let mut recovery = wal.recover(0).unwrap();
    assert_eq!(
        recovery.next().transpose().unwrap(),
        Some((0, Bytes::from_static(b"committed")))
    );
    assert_eq!(
        recovery.next().transpose().unwrap(),
        Some((1, Bytes::from_static(b"after")))
    );
    assert_eq!(recovery.next().transpose().unwrap(), None);
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn torn_header_in_a_non_final_segment_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.max_segment_size = 8192;
    let wal = open_wal(options.clone()).await.unwrap();
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
    first.write_all(b"GARBAGE!!").unwrap();
    first.sync_data().unwrap();

    match open_wal(options).await {
        Ok(_) => panic!("torn non-final segment header unexpectedly opened"),
        Err(error) => assert!(error.to_string().contains("magic")),
    }
}

#[tokio::test]
async fn corruption_in_a_non_final_segment_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.max_segment_size = 8192;
    let wal = open_wal(options.clone()).await.unwrap();
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

    match open_wal(options).await {
        Ok(_) => panic!("corrupt non-final segment unexpectedly opened"),
        Err(error) => assert!(error.to_string().contains("checksum")),
    }
}

#[tokio::test]
async fn rotates_segment_files_without_changing_sequence_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.max_segment_size = 8192;
    let wal = open_wal(options).await.unwrap();

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

    let mut recovery = wal.recover(0).unwrap();
    for value in 0..4u8 {
        assert_eq!(
            recovery.next().transpose().unwrap(),
            Some((value as u64, Bytes::from(vec![value; 128])))
        );
    }
    wal.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_callers_receive_unique_ordered_sequences() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(standard_options(dir.path())).await.unwrap();
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

    let mut recovery = wal.recover(0).unwrap();
    for expected in 0..100 {
        let (sequence, _) = recovery.next().transpose().unwrap().unwrap();
        assert_eq!(sequence, expected);
    }
    assert_eq!(recovery.next().transpose().unwrap(), None);
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn direct_preferred_appends_and_recovers_with_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.io_mode = IoMode::DirectPreferred;
    let wal = open_wal(options).await.unwrap();

    wal.append(Bytes::from_static(b"direct-or-standard"), true)
        .await
        .unwrap();
    let mut recovery = wal.recover(0).unwrap();
    assert_eq!(
        recovery.next().transpose().unwrap(),
        Some((0, Bytes::from_static(b"direct-or-standard")))
    );
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_is_idempotent_and_rejects_new_appends() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(standard_options(dir.path())).await.unwrap();
    wal.shutdown().await.unwrap();
    wal.shutdown().await.unwrap();
    assert!(wal.append(Bytes::from_static(b"late"), true).await.is_err());
}

#[tokio::test]
async fn large_payload_is_fragmented_without_a_record_size_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.max_segment_size = 64 * 1024;
    let wal = open_wal(options.clone()).await.unwrap();
    let payload = Bytes::from(vec![0xA5; 1024 * 1024 + 17]);

    assert_eq!(wal.append(payload.clone(), true).await.unwrap(), 0);
    wal.shutdown().await.unwrap();

    let wal = open_wal(options).await.unwrap();
    let mut recovery = wal.recover(0).unwrap();
    assert_eq!(recovery.next().transpose().unwrap(), Some((0, payload)));
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_syncs_records_requested_as_written() {
    let dir = tempfile::tempdir().unwrap();
    let options = standard_options(dir.path());
    let wal = open_wal(options.clone()).await.unwrap();
    wal.append(Bytes::from_static(b"written"), false)
        .await
        .unwrap();
    wal.shutdown().await.unwrap();

    let wal = open_wal(options).await.unwrap();
    let mut recovery = wal.recover(0).unwrap();
    assert_eq!(
        recovery.next().transpose().unwrap(),
        Some((0, Bytes::from_static(b"written")))
    );
    wal.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_appends_and_shutdown_never_strands_a_caller() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(standard_options(dir.path())).await.unwrap();
    let mut appends = Vec::new();
    for value in 0..100u8 {
        let wal = Arc::clone(&wal);
        appends.push(tokio::spawn(async move {
            wal.append(Bytes::from(vec![value]), true).await
        }));
    }

    let shutdown = {
        let wal = Arc::clone(&wal);
        tokio::spawn(async move { wal.shutdown().await })
    };

    let successful = tokio::time::timeout(Duration::from_secs(5), async {
        let mut successful = BTreeSet::new();
        for append in appends {
            if let Ok(sequence) = append.await.unwrap() {
                successful.insert(sequence);
            }
        }
        shutdown.await.unwrap().unwrap();
        successful
    })
    .await
    .expect("append/shutdown race timed out");

    let mut recovered = BTreeSet::new();
    for (sequence, _) in wal.recover(0).unwrap().map(Result::unwrap) {
        recovered.insert(sequence);
    }
    assert_eq!(recovered, successful);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trim_watch_deletes_only_complete_segments() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.max_segment_size = 8192;
    let (trim_tx, trim_rx) = watch::channel(None);
    let wal = SegmentWal::open(options, trim_rx).await.unwrap();

    for value in 0..4u8 {
        assert_eq!(
            wal.append(Bytes::from(vec![value; 128]), true)
                .await
                .unwrap(),
            value as u64
        );
    }
    assert_eq!(segment_count(dir.path()), 4);

    trim_tx.send(Some(1)).unwrap();
    wait_for_segment_count(dir.path(), 2).await;
    match wal.recover(0) {
        Err(WalError::SequenceExpired {
            requested: 0,
            earliest: 2,
        }) => {}
        Err(error) => panic!("unexpected recovery error: {error}"),
        Ok(_) => panic!("trimmed sequence unexpectedly remained recoverable"),
    }
    let recovered = wal
        .recover(2)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        recovered,
        vec![
            (2, Bytes::from(vec![2; 128])),
            (3, Bytes::from(vec![3; 128]))
        ]
    );

    trim_tx.send(Some(0)).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(segment_count(dir.path()), 2);
    wal.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trim_does_not_rewrite_a_partial_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.max_segment_size = 16 * 1024;
    let (trim_tx, trim_rx) = watch::channel(None);
    let wal = SegmentWal::open(options, trim_rx).await.unwrap();

    for value in 0..4u8 {
        wal.append(Bytes::from(vec![value; 128]), true)
            .await
            .unwrap();
    }
    assert_eq!(segment_count(dir.path()), 2);

    trim_tx.send(Some(0)).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(segment_count(dir.path()), 2);

    trim_tx.send(Some(2)).unwrap();
    wait_for_segment_count(dir.path(), 1).await;
    wal.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_lease_defers_segment_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.max_segment_size = 8192;
    let (trim_tx, trim_rx) = watch::channel(None);
    let wal = SegmentWal::open(options, trim_rx).await.unwrap();

    for value in 0..3u8 {
        wal.append(Bytes::from(vec![value; 128]), true)
            .await
            .unwrap();
    }
    let recovery = wal.recover(0).unwrap();
    trim_tx.send(Some(1)).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(segment_count(dir.path()), 3);

    trim_tx.send(Some(0)).unwrap();
    drop(trim_tx);
    drop(recovery);
    wait_for_segment_count(dir.path(), 1).await;
    wal.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trim_is_clamped_to_the_durable_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.max_segment_size = 8192;
    let (trim_tx, trim_rx) = watch::channel(None);
    let wal = SegmentWal::open(options, trim_rx).await.unwrap();

    wal.append(Bytes::from_static(b"durable"), true)
        .await
        .unwrap();
    wal.append(Bytes::from_static(b"written"), false)
        .await
        .unwrap();
    trim_tx.send(Some(1)).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(segment_count(dir.path()), 2);

    wal.append(Bytes::from_static(b"sync-point"), true)
        .await
        .unwrap();
    wait_for_segment_count(dir.path(), 1).await;
    wal.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initial_trim_on_restart_keeps_sequence_continuity() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.max_segment_size = 8192;
    let (_trim_tx, trim_rx) = watch::channel(None);
    let wal = SegmentWal::open(options.clone(), trim_rx).await.unwrap();

    for value in 0..3u8 {
        wal.append(Bytes::from(vec![value; 128]), true)
            .await
            .unwrap();
    }
    wal.shutdown().await.unwrap();
    assert_eq!(segment_count(dir.path()), 3);

    let (_trim_tx, trim_rx) = watch::channel(Some(2));
    let wal = SegmentWal::open(options, trim_rx).await.unwrap();
    assert_eq!(segment_count(dir.path()), 1);
    assert_eq!(
        wal.append(Bytes::from_static(b"after-restart"), true)
            .await
            .unwrap(),
        3
    );
    wal.shutdown().await.unwrap();
}
