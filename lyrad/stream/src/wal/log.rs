//! Stateful write-ahead log implementation.

use super::error::WalError;
use super::ops::{AppendOp, Operation, SyncOp};
use super::options::LogOptions;
use super::segment::{FileSegment, Segment, list_segments, sync_all};
use super::{Lifecycle, Log, MAX_INFLIGHT_APPEND_NUM, Sequence, WAL_SEGMENT_SIZE};
use crate::vfs::{StandardVfs, Vfs, VfsI};
use async_trait::async_trait;
use bytes::Bytes;
use meta::utils::directory_lock::DirectoryLock;
use meta::utils::logging::utils::log_ignore;
use meta::utils::promise::Promise;
use std::any::Any;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use std::time::Duration;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};

/// A buffered write-ahead log backed by Lyra's segment format.
///
/// A dedicated writer thread assigns sequences and advances the highest
/// written sequence. A second dedicated thread groups synchronization work and
/// advances the highest durable sequence.
pub struct SegmentLog {
    // Control state
    workers: Mutex<Option<WorkerThreads>>,
    close_lock: Mutex<()>,

    // Immutable state
    operation_tx: mpsc::Sender<Operation>,

    // Mutable state
    state: RwLock<Lifecycle>,
}

const CLOSE_TIMEOUT: Duration = Duration::from_secs(30);
const LOCK_FILE_NAME: &str = ".lyra-wal.lock";
const RECOVERY_READ_SIZE: usize = WAL_SEGMENT_SIZE as usize;

struct WorkerThreads {
    writer: WorkerThread,
    sync: WorkerThread,
}

struct WorkerThread {
    // Control state
    handle: Option<JoinHandle<()>>,
    done: oneshot::Receiver<()>,
}

#[derive(Debug)]
struct RecoveredState {
    // Immutable state
    next_sequence: Sequence,
    next_segment_number: u64,
    active_segment: Option<FileSegment>,
}

impl SegmentLog {
    /// Opens the WAL at `options.dir`.
    ///
    /// Existing segments are scanned to restore sequence numbering. A valid
    /// final segment remains active for subsequent appends.
    pub async fn open(options: LogOptions) -> Result<Arc<Self>, WalError> {
        let vfs = VfsI::Standard(StandardVfs);
        vfs.create_dir(&options.dir)?;

        let (operation_tx, operation_rx) = mpsc::channel(MAX_INFLIGHT_APPEND_NUM);
        let (sync_tx, sync_rx) = mpsc::channel(MAX_INFLIGHT_APPEND_NUM);

        let (sync_done_tx, sync_done) = oneshot::channel();
        let sync_handle = ThreadBuilder::new().name("lyra-wal-sync".into()).spawn({
            let dir = options.dir.clone();
            let vfs = vfs.clone();
            move || {
                sync_loop(sync_rx, vfs, dir.as_path());
                tracing::info!("WAL sync thread exited");
                let _ = sync_done_tx.send(());
            }
        })?;

        let (writer_started_tx, writer_started) = oneshot::channel();
        let (writer_done_tx, writer_done) = oneshot::channel();
        let writer_handle = match ThreadBuilder::new().name("lyra-wal-writer".into()).spawn({
            let options = options.clone();
            let vfs = vfs.clone();
            move || {
                match DirectoryLock::acquire(options.dir.join(LOCK_FILE_NAME))
                    .map_err(|error| {
                        if error.kind() == ErrorKind::WouldBlock {
                            WalError::Locked(options.dir.clone())
                        } else {
                            error.into()
                        }
                    })
                    .and_then(|directory_lock| {
                        recover_state(&vfs, &options).map(|recovered| (directory_lock, recovered))
                    }) {
                    Ok((directory_lock, recovered)) => {
                        if writer_started_tx.send(Ok(())).is_ok() {
                            writer_loop(operation_rx, sync_tx, vfs, options, recovered);
                        }
                        drop(directory_lock);
                    }
                    Err(error) => {
                        let _ = writer_started_tx.send(Err(error));
                    }
                }
                tracing::info!("WAL writer thread exited");
                let _ = writer_done_tx.send(());
            }
        }) {
            Ok(handle) => handle,
            Err(error) => {
                let _ = sync_done.await;
                let _ = sync_handle.join();
                return Err(error.into());
            }
        };

        match writer_started.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let _ = writer_done.await;
                let _ = sync_done.await;
                let _ = writer_handle.join();
                let _ = sync_handle.join();
                return Err(error);
            }
            Err(_) => {
                let _ = writer_done.await;
                let _ = sync_done.await;
                let error = writer_handle
                    .join()
                    .err()
                    .map(panic_message)
                    .unwrap_or_else(|| "WAL writer thread stopped during startup".into());
                let _ = sync_handle.join();
                return Err(WalError::Worker(error));
            }
        }

        Ok(Arc::new(Self {
            workers: Mutex::new(Some(WorkerThreads {
                writer: WorkerThread {
                    handle: Some(writer_handle),
                    done: writer_done,
                },
                sync: WorkerThread {
                    handle: Some(sync_handle),
                    done: sync_done,
                },
            })),
            close_lock: Mutex::new(()),
            operation_tx,
            state: RwLock::new(Lifecycle::Running),
        }))
    }
}

#[async_trait]
impl Log for SegmentLog {
    /// Eagerly submits `payload` and returns its eventual assigned sequence.
    ///
    /// The returned promise follows the log's configured sync policy. Dropping
    /// it discards only the result and does not cancel the accepted append.
    fn append(&self, payload: Bytes) -> Promise<Sequence, WalError> {
        let (handle, promise) = Promise::new();
        let Ok(state) = self.state.try_read() else {
            handle.finish(Err(WalError::Closed));
            return promise;
        };
        if *state != Lifecycle::Running {
            handle.finish(Err(WalError::Closed));
            return promise;
        }

        match self
            .operation_tx
            .try_send(Operation::Append(AppendOp { payload, handle }))
        {
            Ok(()) => {}
            Err(TrySendError::Full(Operation::Append(append_op))) => {
                append_op.handle.finish(Err(WalError::QueueFull));
            }
            Err(TrySendError::Closed(Operation::Append(append_op))) => {
                append_op.handle.finish(Err(WalError::Closed));
            }
            Err(_) => unreachable!("append queue returned a non-append operation"),
        }
        promise
    }

    /// Stops admission, drains owned work, and closes all components.
    ///
    /// Close is idempotent and best effort: failures are logged while later,
    /// independent cleanup stages continue.
    async fn close(&self) {
        let _close_guard = self.close_lock.lock().await;
        {
            let mut state = self.state.write().await;
            if *state == Lifecycle::Closed {
                return;
            }
            *state = Lifecycle::Closing;
        }

        log_ignore!(
            "send-writer-close",
            self.operation_tx
                .send(Operation::Close)
                .await
                .map_err(|_| WalError::Closed)
        );

        let mut workers_guard = self.workers.lock().await;
        if let Some(workers) = workers_guard.as_mut() {
            let writer_stopped = wait_for_worker("writer", &mut workers.writer).await;
            let sync_stopped = wait_for_worker("sync", &mut workers.sync).await;
            if let Some(mut workers) = workers_guard.take() {
                finish_worker("writer", &mut workers.writer, writer_stopped);
                finish_worker("sync", &mut workers.sync, sync_stopped);
            }
        }
        drop(workers_guard);

        *self.state.write().await = Lifecycle::Closed;
    }
}

async fn wait_for_worker(name: &'static str, worker: &mut WorkerThread) -> bool {
    match tokio::time::timeout(CLOSE_TIMEOUT, &mut worker.done).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            tracing::error!(worker = name, error = %error, "worker completion signal failed");
            true
        }
        Err(error) => {
            tracing::error!(worker = name, error = %error, "worker close timed out; detaching it");
            false
        }
    }
}

fn finish_worker(name: &'static str, worker: &mut WorkerThread, stopped: bool) {
    let Some(handle) = worker.handle.take() else {
        return;
    };
    if stopped {
        log_ignore!(
            "join-worker",
            handle
                .join()
                .map_err(|panic| WalError::Worker(format!("{name}: {}", panic_message(panic))))
        );
    } else {
        drop(handle);
    }
}

fn writer_loop(
    mut operation_rx: mpsc::Receiver<Operation>,
    sync_tx: mpsc::Sender<Operation>,
    vfs: VfsI,
    options: LogOptions,
    recovered: RecoveredState,
) {
    let RecoveredState {
        mut next_sequence,
        mut next_segment_number,
        mut active_segment,
    } = recovered;
    let mut dirty_segments = Vec::new();
    let mut directory_dirty = false;

    'writer: loop {
        let mut operations = Vec::new();
        let received = operation_rx.blocking_recv_many(&mut operations, MAX_INFLIGHT_APPEND_NUM);
        if received == 0 {
            break;
        }

        let mut write_then_close = false;
        for operation in operations {
            match operation {
                Operation::Append(append_op) => {
                    let AppendOp { payload, handle } = append_op;
                    let mut handle = Some(handle);
                    let sequence = next_sequence;
                    let Some(incremented_sequence) = sequence.checked_add(1) else {
                        let error = WalError::Worker("WAL sequence space exhausted".into());
                        handle.take().unwrap().finish(Err(error));
                        break 'writer;
                    };
                    next_sequence = incremented_sequence;

                    let mut record = Vec::with_capacity(8 + payload.len());
                    record.extend_from_slice(&sequence.to_le_bytes());
                    record.extend_from_slice(&payload);
                    let write_result = (|| -> Result<(), WalError> {
                        loop {
                            if active_segment.is_none() {
                                let number = next_segment_number;
                                let incremented_number =
                                    number.checked_add(1).ok_or_else(|| {
                                        WalError::Worker(
                                            "WAL segment number space exhausted".into(),
                                        )
                                    })?;
                                active_segment = Some(FileSegment::create(
                                    &vfs,
                                    &options.dir,
                                    number,
                                    WAL_SEGMENT_SIZE,
                                )?);
                                next_segment_number = incremented_number;
                                directory_dirty = true;
                            }

                            match active_segment.as_ref().unwrap().append(&record) {
                                Ok(()) => break,
                                Err(WalError::SegmentFull) => {
                                    dirty_segments.push(active_segment.take().unwrap());
                                }
                                Err(error) => return Err(error),
                            }
                        }
                        Ok(())
                    })();
                    if let Err(error) = write_result {
                        tracing::error!(sequence, error = %error, "WAL write failed");
                        handle.take().unwrap().finish(Err(error));
                        break 'writer;
                    }

                    if let Some(segment) = active_segment.as_ref() {
                        dirty_segments.push(segment.clone());
                    }
                    let sync_op = SyncOp {
                        segments: std::mem::take(&mut dirty_segments),
                        sync_directory: std::mem::take(&mut directory_dirty),
                        completion: options.sync.then(|| (sequence, handle.take().unwrap())),
                    };
                    if let Err(send_error) = sync_tx.blocking_send(Operation::Sync(sync_op)) {
                        let error = WalError::Worker("WAL sync thread stopped".into());
                        tracing::error!(sequence, error = %error);
                        let Operation::Sync(mut sync_op) = send_error.0 else {
                            unreachable!("sync queue returned a non-sync operation")
                        };
                        let handle = handle
                            .take()
                            .or_else(|| sync_op.completion.take().map(|(_, handle)| handle))
                            .unwrap();
                        handle.finish(Err(error));
                        break 'writer;
                    }

                    if let Some(handle) = handle {
                        handle.finish(Ok(sequence));
                    }
                }
                Operation::Sync(_) => unreachable!("sync operation sent to WAL writer"),
                Operation::Close => {
                    operation_rx.close();
                    write_then_close = true;
                }
            }
        }

        if write_then_close {
            break;
        }
    }

    if sync_tx.blocking_send(Operation::Close).is_err() {
        tracing::error!("sync-operation queue closed before the close operation was delivered");
    }
}

fn sync_loop(mut sync_rx: mpsc::Receiver<Operation>, vfs: VfsI, dir: &Path) {
    loop {
        let mut operations = Vec::new();
        let received = sync_rx.blocking_recv_many(&mut operations, MAX_INFLIGHT_APPEND_NUM);
        if received == 0 {
            break;
        }

        let mut segments = Vec::new();
        let mut sync_dir = false;
        let mut completions = Vec::new();
        let mut closing = false;
        for operation in operations {
            match operation {
                Operation::Sync(sync_op) => {
                    segments.extend(sync_op.segments);
                    sync_dir |= sync_op.sync_directory;
                    completions.extend(sync_op.completion);
                }
                Operation::Append(_) => unreachable!("append operation sent to WAL sync worker"),
                Operation::Close => {
                    sync_rx.close();
                    closing = true;
                }
            }
        }

        if !segments.is_empty() {
            match perform_sync(&segments, sync_dir, &vfs, dir) {
                Ok(()) => {
                    for (sequence, handle) in completions {
                        handle.finish(Ok(sequence));
                    }
                }
                Err(error) => {
                    tracing::error!(error = %error, "WAL sync failed");
                    for (_, handle) in completions {
                        handle.finish(Err(error.clone()));
                    }
                    break;
                }
            }
        }

        if closing {
            break;
        }
    }
}

/// Scans the WAL, repairs a truncated final record, and restores the final
/// segment as the active buffered segment.
fn recover_state(vfs: &VfsI, options: &LogOptions) -> Result<RecoveredState, WalError> {
    let mut next_sequence = 0;
    let mut max_segment_number: u64 = 0;
    let segment_files = list_segments(vfs, &options.dir)?;
    let segment_count = segment_files.len();
    let mut active_segment = None;

    for (index, (file_number, path)) in segment_files.into_iter().enumerate() {
        let final_segment = index + 1 == segment_count;
        max_segment_number = max_segment_number.max(file_number);

        let file_size = std::fs::metadata(&path)?.len();
        if file_size > WAL_SEGMENT_SIZE {
            return Err(WalError::Corruption {
                path,
                message: format!(
                    "segment size {} exceeds the maximum {WAL_SEGMENT_SIZE}",
                    file_size
                ),
            });
        }

        let segment = FileSegment::open(vfs, &path, file_number, WAL_SEGMENT_SIZE, file_size)?;
        let mut position = 0;
        while position < file_size {
            let (next_position, records) = match segment.read(position, RECOVERY_READ_SIZE) {
                Ok(records) => records,
                Err(WalError::Truncated { .. }) if final_segment => {
                    if position < file_size {
                        segment.truncate(position)?;
                        segment.sync()?;
                    }
                    tracing::warn!(
                        path = %path.display(),
                        valid_size = position,
                        "truncated incomplete final WAL record during recovery"
                    );
                    break;
                }
                Err(WalError::Truncated { path, message }) => {
                    return Err(WalError::Corruption { path, message });
                }
                Err(error) => return Err(error),
            };
            if records.is_empty() {
                return Err(WalError::Worker(
                    "WAL segment read made no recovery progress".into(),
                ));
            }

            for record in records {
                let sequence = decode_sequence(&path, &record)?;
                if sequence != next_sequence {
                    return Err(WalError::Corruption {
                        path: path.clone(),
                        message: format!("expected WAL sequence {next_sequence}, found {sequence}"),
                    });
                }
                next_sequence = next_sequence
                    .checked_add(1)
                    .ok_or_else(|| WalError::Worker("WAL sequence space exhausted".into()))?;
            }
            position = next_position;
        }
        if final_segment {
            active_segment = Some(segment);
        }
    }

    let next_segment_number = max_segment_number
        .checked_add(1)
        .ok_or_else(|| WalError::Worker("WAL segment number space exhausted".into()))?;
    Ok(RecoveredState {
        // Immutable state
        next_sequence,
        next_segment_number,
        active_segment,
    })
}

fn decode_sequence(path: &Path, record: &[u8]) -> Result<Sequence, WalError> {
    let prefix = record.get(..8).ok_or_else(|| WalError::Corruption {
        path: path.to_path_buf(),
        message: "record shorter than the sequence prefix".into(),
    })?;
    Ok(u64::from_le_bytes(prefix.try_into().unwrap()))
}

fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "WAL worker thread panicked".into()
    }
}

fn perform_sync(
    segments: &[FileSegment],
    sync_dir: bool,
    vfs: &VfsI,
    dir: &Path,
) -> Result<(), WalError> {
    let mut previous = None;
    for segment in segments.iter().rev() {
        if previous.is_some_and(|previous| previous == segment) {
            continue;
        }
        segment.sync()?;
        previous = Some(segment);
    }
    if sync_dir {
        sync_all(vfs, dir)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    fn standard_options(path: &Path) -> LogOptions {
        LogOptions::new(path, true)
    }

    fn seed_records(
        options: &LogOptions,
        records: &[(Sequence, Bytes)],
        max_records_size: u64,
    ) -> Result<(), WalError> {
        let vfs = VfsI::Standard(StandardVfs);
        let mut next_segment_number = 1;
        let mut active_segment: Option<FileSegment> = None;
        let mut dirty_segments = Vec::new();

        for (sequence, payload) in records {
            let mut record = Vec::with_capacity(8 + payload.len());
            record.extend_from_slice(&sequence.to_le_bytes());
            record.extend_from_slice(payload);
            loop {
                if active_segment.is_none() {
                    active_segment = Some(FileSegment::create(
                        &vfs,
                        &options.dir,
                        next_segment_number,
                        max_records_size,
                    )?);
                    next_segment_number += 1;
                }
                match active_segment.as_ref().unwrap().append(&record) {
                    Ok(()) => break,
                    Err(WalError::SegmentFull) => {
                        dirty_segments.push(active_segment.take().unwrap());
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        if let Some(segment) = active_segment.as_ref() {
            dirty_segments.push(segment.clone());
        }
        perform_sync(&dirty_segments, true, &vfs, &options.dir)?;
        Ok(())
    }

    fn seed_segments(options: &LogOptions, payloads: &[Bytes], max_records_size: u64) {
        let records: Vec<_> = payloads
            .iter()
            .enumerate()
            .map(|(sequence, payload)| (sequence as Sequence, payload.clone()))
            .collect();
        seed_records(options, &records, max_records_size).unwrap();
    }

    #[tokio::test]
    async fn recovers_sequence_across_rotated_segments() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        let payloads: Vec<_> = (0..4u8)
            .map(|value| Bytes::from(vec![value; 128]))
            .collect();
        seed_segments(&options, &payloads, 147);

        assert_eq!(
            list_segments(&VfsI::Standard(StandardVfs), dir.path())
                .unwrap()
                .len(),
            4
        );
        let log = SegmentLog::open(options).await.unwrap();
        assert_eq!(log.append(Bytes::from_static(b"next")).await.unwrap(), 4);
        log.close().await;
    }

    #[test]
    fn a_record_cannot_exceed_the_record_area_size() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        let payload = Bytes::from(vec![0xA5; 1024 * 1024 + 17]);
        let error = seed_records(&options, &[(0, payload)], 64 * 1024).unwrap_err();
        assert!(matches!(error, WalError::RecordTooLarge { .. }));
    }

    #[tokio::test]
    async fn recovery_rejects_a_torn_nonfinal_segment() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        let payloads = vec![Bytes::from(vec![0x11; 128]), Bytes::from(vec![0x22; 128])];
        seed_segments(&options, &payloads, 147);

        let files = list_segments(&VfsI::Standard(StandardVfs), dir.path()).unwrap();
        assert_eq!(files.len(), 2);
        OpenOptions::new()
            .write(true)
            .open(&files[0].1)
            .unwrap()
            .set_len(4)
            .unwrap();

        let error = SegmentLog::open(options).await.err().unwrap();
        assert!(matches!(error, WalError::Corruption { .. }));
        assert_eq!(std::fs::metadata(&files[0].1).unwrap().len(), 4);
    }

    #[tokio::test]
    async fn recovery_truncates_a_torn_final_segment_and_continues() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        let payloads = vec![Bytes::from(vec![0x11; 128]), Bytes::from(vec![0x22; 128])];
        seed_segments(&options, &payloads, WAL_SEGMENT_SIZE);

        let path = list_segments(&VfsI::Standard(StandardVfs), dir.path()).unwrap()[0]
            .1
            .clone();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(151)
            .unwrap();

        let log = SegmentLog::open(options.clone()).await.unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 147);
        assert_eq!(
            log.append(Bytes::from_static(b"replacement"))
                .await
                .unwrap(),
            1
        );
        log.close().await;

        let reopened = SegmentLog::open(options).await.unwrap();
        assert_eq!(
            reopened.append(Bytes::from_static(b"next")).await.unwrap(),
            2
        );
        reopened.close().await;
    }

    #[tokio::test]
    async fn recovery_rejects_checksum_corruption_in_the_final_segment() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        seed_segments(&options, &[Bytes::from_static(b"record")], WAL_SEGMENT_SIZE);

        let path = list_segments(&VfsI::Standard(StandardVfs), dir.path()).unwrap()[0]
            .1
            .clone();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[11] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();
        let file_size = std::fs::metadata(&path).unwrap().len();

        let error = SegmentLog::open(options).await.err().unwrap();
        assert!(matches!(
            error,
            WalError::Corruption { message, .. }
                if message == "physical record checksum mismatch"
        ));
        assert_eq!(std::fs::metadata(path).unwrap().len(), file_size);
    }

    #[test]
    fn recovery_rejects_a_sequence_gap() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        let records = [
            (0, Bytes::from_static(b"zero")),
            (2, Bytes::from_static(b"two")),
        ];

        seed_records(&options, &records, WAL_SEGMENT_SIZE).unwrap();

        let error = recover_state(&VfsI::Standard(StandardVfs), &options).unwrap_err();
        assert!(matches!(
            error,
            WalError::Corruption { message, .. }
                if message == "expected WAL sequence 1, found 2"
        ));
    }

    #[test]
    fn recovery_rejects_a_segment_filename_record_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        seed_segments(&options, &[Bytes::from_static(b"record")], WAL_SEGMENT_SIZE);
        let (_, original_path) = list_segments(&VfsI::Standard(StandardVfs), dir.path())
            .unwrap()
            .pop()
            .unwrap();
        let renamed_path = dir.path().join("0000000002.seg");
        std::fs::rename(original_path, &renamed_path).unwrap();

        let error = recover_state(&VfsI::Standard(StandardVfs), &options).unwrap_err();
        assert!(matches!(
            error,
            WalError::Corruption { path, message }
                if path == renamed_path
                    && message == "physical record segment number mismatch"
        ));
    }
}
