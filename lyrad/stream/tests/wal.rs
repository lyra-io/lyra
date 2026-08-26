use bytes::Bytes;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use stream::{Log, LogOptions, SegmentLog, WalError};

fn standard_options(path: &Path) -> LogOptions {
    LogOptions::new(path, true)
}

async fn open_wal(options: LogOptions) -> Result<Arc<dyn Log>, WalError> {
    Ok(SegmentLog::open(options).await?)
}

fn segment_count(path: &Path) -> usize {
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

    assert_eq!(wal.append(Bytes::from_static(b"one")).await.unwrap(), 0);
    assert_eq!(wal.append(Bytes::new()).await.unwrap(), 1);
    assert_eq!(wal.append(Bytes::from_static(b"three")).await.unwrap(), 2);
    wal.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_callers_receive_unique_ordered_sequences() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(standard_options(dir.path())).await.unwrap();
    let mut tasks = Vec::new();

    for value in 0..100u8 {
        let wal = Arc::clone(&wal);
        tasks.push(tokio::spawn(async move {
            wal.append(Bytes::from(vec![value])).await.unwrap()
        }));
    }

    let mut sequences = BTreeSet::new();
    for task in tasks {
        sequences.insert(task.await.unwrap());
    }
    assert_eq!(sequences, (0..100).collect());
    wal.close().await;
}

#[tokio::test]
async fn buffered_wal_appends() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(standard_options(dir.path())).await.unwrap();

    wal.append(Bytes::from_static(b"buffered")).await.unwrap();
    wal.close().await;
}

#[tokio::test]
async fn close_is_idempotent_and_rejects_new_appends() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(standard_options(dir.path())).await.unwrap();
    wal.close().await;
    wal.close().await;
    assert!(wal.append(Bytes::from_static(b"late")).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_close_callers_all_complete_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(standard_options(dir.path())).await.unwrap();
    let mut closes = Vec::new();

    for _ in 0..16 {
        let wal = Arc::clone(&wal);
        closes.push(tokio::spawn(async move { wal.close().await }));
    }

    for close in closes {
        close.await.unwrap();
    }
    assert_eq!(
        wal.append(Bytes::from_static(b"late")).await,
        Err(WalError::Closed)
    );
}

#[tokio::test]
async fn only_one_log_instance_can_own_a_directory() {
    let dir = tempfile::tempdir().unwrap();
    let options = standard_options(dir.path());
    let first = open_wal(options.clone()).await.unwrap();

    let error = open_wal(options.clone()).await.err().unwrap();
    assert_eq!(error, WalError::Locked(dir.path().to_path_buf()));

    first.close().await;
    let reopened = open_wal(options).await.unwrap();
    reopened.close().await;
}

#[tokio::test]
async fn recovery_failure_releases_the_directory_lock() {
    let dir = tempfile::tempdir().unwrap();
    let options = standard_options(dir.path());
    let malformed = dir.path().join("1.seg");
    std::fs::write(&malformed, []).unwrap();

    assert!(matches!(
        open_wal(options.clone()).await,
        Err(WalError::Corruption { path, .. }) if path == malformed
    ));

    std::fs::remove_file(malformed).unwrap();
    let reopened = open_wal(options).await.unwrap();
    reopened.close().await;
}

#[tokio::test]
async fn appends_a_large_payload_within_the_record_area_limit() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(standard_options(dir.path())).await.unwrap();
    let payload = Bytes::from(vec![0xA5; 1024 * 1024 + 17]);

    assert_eq!(wal.append(payload).await.unwrap(), 0);
    wal.close().await;
}

#[tokio::test]
async fn close_flushes_unsynced_appends() {
    let dir = tempfile::tempdir().unwrap();
    let options = LogOptions::new(dir.path(), false);
    let wal = open_wal(options.clone()).await.unwrap();

    for value in 0..3u8 {
        assert_eq!(
            wal.append(Bytes::from(vec![value])).await.unwrap(),
            value as u64
        );
    }
    wal.close().await;

    let reopened = open_wal(options).await.unwrap();
    assert_eq!(
        reopened.append(Bytes::from_static(b"next")).await.unwrap(),
        3
    );
    reopened.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_failure_does_not_strand_its_caller() {
    let root = tempfile::tempdir().unwrap();
    let wal_dir = root.path().join("wal");
    let moved_dir = root.path().join("moved");
    let wal = open_wal(standard_options(&wal_dir)).await.unwrap();
    std::fs::rename(&wal_dir, moved_dir).unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        wal.append(Bytes::from_static(b"blocked")),
    )
    .await
    .expect("append timed out");
    assert!(matches!(result, Err(WalError::Io(_))));

    tokio::time::timeout(Duration::from_secs(5), wal.close())
        .await
        .expect("close timed out");
}

#[tokio::test]
async fn reopen_recovers_sequences() {
    let dir = tempfile::tempdir().unwrap();
    let options = standard_options(dir.path());
    {
        let wal = open_wal(options.clone()).await.unwrap();
        for value in 0..3u8 {
            assert_eq!(
                wal.append(Bytes::from(vec![value])).await.unwrap(),
                value as u64
            );
        }
        wal.close().await;
    }
    assert_eq!(segment_count(dir.path()), 1);

    let wal = open_wal(options).await.unwrap();
    // Sequence numbering resumes after the last durable record.
    assert_eq!(wal.append(Bytes::from_static(b"next")).await.unwrap(), 3);
    wal.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_appends_and_close_never_strands_a_caller() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open_wal(standard_options(dir.path())).await.unwrap();
    let mut appends = Vec::new();
    for value in 0..100u8 {
        let wal = Arc::clone(&wal);
        appends.push(tokio::spawn(async move {
            wal.append(Bytes::from(vec![value])).await
        }));
    }

    let close = {
        let wal = Arc::clone(&wal);
        tokio::spawn(async move { wal.close().await })
    };

    let successful = tokio::time::timeout(Duration::from_secs(5), async {
        let mut successful = BTreeSet::new();
        for append in appends {
            if let Ok(sequence) = append.await.unwrap() {
                successful.insert(sequence);
            }
        }
        close.await.unwrap();
        successful
    })
    .await
    .expect("append/close race timed out");

    assert_eq!(
        successful,
        (0..successful.len() as u64).collect::<BTreeSet<_>>()
    );
}
