use bytes::Bytes;
use lyrad::wal::{IoMode, SegmentWal, Wal, WalError, WalOptions};
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

fn standard_options(path: &std::path::Path) -> WalOptions {
    let mut options = WalOptions::new(path);
    options.io_mode = IoMode::Standard;
    options
}

async fn open_wal(options: WalOptions) -> Result<Arc<SegmentWal>, WalError> {
    SegmentWal::open(options).await
}

fn segment_count(path: &std::path::Path) -> usize {
    std::fs::read_dir(path)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "seg"))
        .count()
}

#[tokio::test]
async fn appends_return_sequential_sequences() {
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
    wal.shutdown().await.unwrap();
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
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn direct_preferred_appends_with_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.io_mode = IoMode::DirectPreferred;
    let wal = open_wal(options).await.unwrap();

    wal.append(Bytes::from_static(b"direct-or-standard"), true)
        .await
        .unwrap();
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
async fn large_payload_appends_without_a_record_size_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut options = standard_options(dir.path());
    options.max_segment_size = 64 * 1024;
    let wal = open_wal(options).await.unwrap();
    let payload = Bytes::from(vec![0xA5; 1024 * 1024 + 17]);

    assert_eq!(wal.append(payload, true).await.unwrap(), 0);
    wal.shutdown().await.unwrap();
}

#[tokio::test]
async fn open_fails_when_directory_contains_existing_segments() {
    let dir = tempfile::tempdir().unwrap();
    let options = standard_options(dir.path());
    let wal = open_wal(options.clone()).await.unwrap();
    wal.append(Bytes::from_static(b"written"), true)
        .await
        .unwrap();
    wal.shutdown().await.unwrap();
    assert_eq!(segment_count(dir.path()), 1);

    match open_wal(options).await {
        Ok(_) => panic!("WAL with existing segments unexpectedly reopened"),
        Err(WalError::ExistingSegments { .. }) => {}
        Err(error) => panic!("unexpected open error: {error}"),
    }
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

    assert_eq!(
        successful,
        (0..successful.len() as u64).collect::<BTreeSet<_>>()
    );
}
