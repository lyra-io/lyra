//! WAL append and recovery event loop.

use super::error::WalError;
use super::log_syncer::LogSyncHandle;
use super::ops::{
    AdvancedSequence, AppendCompletion, AppendHandle, AppendOp, DirtySegmentQueue, Operation,
};
use super::options::LogOptions;
use super::segment::{FileSegment, Segment, list_segments};
use super::{Lifecycle, MAX_INFLIGHT_APPEND_NUM, Sequence, WAL_SEGMENT_SIZE};
use crate::vfs::{Vfs, VfsI};
use bytes::Bytes;
use meta::utils::directory_lock::DirectoryLock;
use meta::utils::logging::utils::log_ignore;
use meta::utils::promise::Promise;
use std::any::Any;
use std::collections::VecDeque;
use std::io::ErrorKind;
use std::path::Path;
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use std::time::Duration;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot, watch};

const CLOSE_TIMEOUT: Duration = Duration::from_secs(30);
const LOCK_FILE_NAME: &str = ".lyra-wal.lock";
const RECOVERY_READ_SIZE: usize = WAL_SEGMENT_SIZE as usize;

#[derive(Debug)]
struct RecoveredState {
    // Immutable state
    next_sequence: Sequence,
    next_segment_number: u64,
    active_segment: Option<FileSegment>,
}

struct PendingAppend {
    // Immutable state
    sequence: Sequence,
    sequence_bytes: [u8; 8],
    payload: Bytes,
    handle: AppendHandle,
}

/// Controls WAL append admission and the writer thread.
pub(super) struct LogWriter {
    // Control state
    worker: Mutex<Option<WriterWorker>>,
    close_lock: Mutex<()>,

    // Immutable state
    operation_tx: mpsc::Sender<Operation>,

    // Mutable state
    state: RwLock<Lifecycle>,
}

struct WriterWorker {
    // Control state
    handle: Option<JoinHandle<()>>,
    done: oneshot::Receiver<()>,
}

/// Assigns sequences and appends accepted operations to WAL segments.
struct WriterLoop {
    // Control state
    operation_rx: mpsc::Receiver<Operation>,

    // Immutable state
    advanced_tx: watch::Sender<AdvancedSequence>,
    dirty_segments: DirtySegmentQueue,
    completion_tx: mpsc::Sender<AppendCompletion>,
    vfs: VfsI,
    options: LogOptions,

    // Mutable state
    next_sequence: Sequence,
    next_segment_number: u64,
    active_segment: Option<FileSegment>,
    operations: Vec<Operation>,
    pending: VecDeque<PendingAppend>,
    record: Vec<u8>,
}

impl LogWriter {
    pub(super) async fn new(
        vfs: VfsI,
        options: LogOptions,
        sync_handle: LogSyncHandle,
    ) -> Result<Self, WalError> {
        vfs.create_dir(&options.dir)?;
        let (operation_tx, operation_rx) = mpsc::channel(MAX_INFLIGHT_APPEND_NUM);
        let LogSyncHandle {
            advanced_tx,
            dirty_segments,
            completion_tx,
        } = sync_handle;
        let (started_tx, started) = oneshot::channel();
        let (done_tx, done) = oneshot::channel();
        let handle = ThreadBuilder::new().name("lyra-wal-writer".into()).spawn({
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
                        WriterLoop::new(
                            operation_rx,
                            advanced_tx,
                            dirty_segments,
                            completion_tx,
                            vfs,
                            options,
                        )
                        .map(|writer_loop| (directory_lock, writer_loop))
                    }) {
                    Ok((directory_lock, writer_loop)) => {
                        if started_tx.send(Ok(())).is_ok() {
                            writer_loop.run();
                        }
                        drop(directory_lock);
                    }
                    Err(error) => {
                        let _ = started_tx.send(Err(error));
                    }
                }
                tracing::info!("WAL writer thread exited");
                let _ = done_tx.send(());
            }
        })?;

        match started.await {
            Ok(Ok(())) => Ok(Self {
                // Control state
                worker: Mutex::new(Some(WriterWorker {
                    handle: Some(handle),
                    done,
                })),
                close_lock: Mutex::new(()),

                // Immutable state
                operation_tx,

                // Mutable state
                state: RwLock::new(Lifecycle::Running),
            }),
            Ok(Err(error)) => {
                let _ = done.await;
                let _ = handle.join();
                Err(error)
            }
            Err(_) => {
                let _ = done.await;
                let error = handle
                    .join()
                    .err()
                    .map(panic_message)
                    .unwrap_or_else(|| "WAL writer thread stopped during startup".into());
                Err(WalError::Worker(error))
            }
        }
    }

    pub(super) fn append(&self, payload: Bytes) -> Promise<Sequence, WalError> {
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

    pub(super) async fn close(&self) {
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

        let mut worker_guard = self.worker.lock().await;
        if let Some(mut worker) = worker_guard.take() {
            let stopped = match tokio::time::timeout(CLOSE_TIMEOUT, &mut worker.done).await {
                Ok(Ok(())) => true,
                Ok(Err(error)) => {
                    tracing::error!(worker = "writer", error = %error, "worker completion signal failed");
                    true
                }
                Err(error) => {
                    tracing::error!(worker = "writer", error = %error, "worker close timed out; detaching it");
                    false
                }
            };
            if let Some(handle) = worker.handle.take()
                && stopped
            {
                log_ignore!(
                    "join-writer-worker",
                    handle.join().map_err(|panic| {
                        WalError::Worker(format!("writer: {}", panic_message(panic)))
                    })
                );
            }
        }
        *self.state.write().await = Lifecycle::Closed;
    }
}

impl WriterLoop {
    fn new(
        operation_rx: mpsc::Receiver<Operation>,
        advanced_tx: watch::Sender<AdvancedSequence>,
        dirty_segments: DirtySegmentQueue,
        completion_tx: mpsc::Sender<AppendCompletion>,
        vfs: VfsI,
        options: LogOptions,
    ) -> Result<Self, WalError> {
        let RecoveredState {
            next_sequence,
            next_segment_number,
            active_segment,
        } = recover_state(&vfs, &options)?;
        Ok(Self {
            // Control state
            operation_rx,

            // Immutable state
            advanced_tx,
            dirty_segments,
            completion_tx,
            vfs,
            options,

            // Mutable state
            next_sequence,
            next_segment_number,
            active_segment,
            operations: Vec::with_capacity(MAX_INFLIGHT_APPEND_NUM),
            pending: VecDeque::with_capacity(MAX_INFLIGHT_APPEND_NUM),
            record: Vec::new(),
        })
    }

    fn run(self) {
        let Self {
            mut operation_rx,
            advanced_tx,
            dirty_segments,
            completion_tx,
            vfs,
            options,
            mut next_sequence,
            mut next_segment_number,
            mut active_segment,
            mut operations,
            mut pending,
            mut record,
        } = self;

        loop {
            operations.clear();
            pending.clear();
            let received =
                operation_rx.blocking_recv_many(&mut operations, MAX_INFLIGHT_APPEND_NUM);
            if received == 0 {
                break;
            }

            let mut write_then_close = false;
            let mut stop_writer = false;
            let mut batch_advanced_sequence = None;
            for operation in operations.drain(..) {
                match operation {
                    Operation::Append(append_op) => {
                        let AppendOp { payload, handle } = append_op;
                        let sequence = next_sequence;
                        let Some(incremented_sequence) = sequence.checked_add(1) else {
                            let error = WalError::Worker("WAL sequence space exhausted".into());
                            handle.finish(Err(error));
                            stop_writer = true;
                            break;
                        };
                        next_sequence = incremented_sequence;
                        pending.push_back(PendingAppend {
                            // Immutable state
                            sequence,
                            sequence_bytes: sequence.to_le_bytes(),
                            payload,
                            handle,
                        });
                    }
                    Operation::Close => {
                        operation_rx.close();
                        write_then_close = true;
                    }
                }
            }

            while !pending.is_empty() {
                if active_segment.is_none() {
                    let number = next_segment_number;
                    let Some(incremented_number) = number.checked_add(1) else {
                        let error = WalError::Worker("WAL segment number space exhausted".into());
                        tracing::error!(error = %error);
                        for append in pending.drain(..) {
                            append.handle.finish(Err(error.clone()));
                        }
                        stop_writer = true;
                        break;
                    };
                    match FileSegment::create(&vfs, &options.dir, number, WAL_SEGMENT_SIZE) {
                        Ok(segment) => {
                            active_segment = Some(segment);
                            next_segment_number = incremented_number;
                        }
                        Err(error) => {
                            tracing::error!(error = %error, "WAL segment creation failed");
                            for append in pending.drain(..) {
                                append.handle.finish(Err(error.clone()));
                            }
                            stop_writer = true;
                            break;
                        }
                    }
                }

                let append_result = if pending.len() == 1 {
                    let append = pending.front().unwrap();
                    record.clear();
                    record.reserve(append.sequence_bytes.len() + append.payload.len());
                    record.extend_from_slice(&append.sequence_bytes);
                    record.extend_from_slice(&append.payload);
                    active_segment.as_ref().unwrap().append(&record).map(|()| 1)
                } else {
                    active_segment.as_ref().unwrap().append_batch(
                        pending.iter().map(|append| {
                            (append.sequence_bytes.as_slice(), append.payload.as_ref())
                        }),
                    )
                };
                let appended = match append_result {
                    Ok(0) => {
                        let error = WalError::Worker("WAL segment batch made no progress".into());
                        tracing::error!(error = %error);
                        for append in pending.drain(..) {
                            append.handle.finish(Err(error.clone()));
                        }
                        stop_writer = true;
                        break;
                    }
                    Ok(appended) => appended,
                    Err(WalError::SegmentFull) => {
                        let segment = active_segment.take().unwrap();
                        dirty_segments
                            .lock()
                            .unwrap()
                            .push_back(segment.sync_handle());
                        continue;
                    }
                    Err(error) => {
                        let sequence = pending.front().unwrap().sequence;
                        tracing::error!(sequence, error = %error, "WAL write failed");
                        for append in pending.drain(..) {
                            append.handle.finish(Err(error.clone()));
                        }
                        stop_writer = true;
                        break;
                    }
                };

                for _ in 0..appended {
                    let append = pending.pop_front().unwrap();
                    let completion = (append.sequence, append.handle);
                    match completion_tx.try_send(completion) {
                        Ok(()) => {}
                        Err(TrySendError::Full(completion)) => {
                            // Wake the syncer before blocking so it can drain
                            // the bounded completion queue.
                            if let Some(advanced_sequence) = batch_advanced_sequence.take() {
                                let active = active_segment.as_ref().map(FileSegment::sync_handle);
                                if advanced_tx.send((Some(advanced_sequence), active)).is_err() {
                                    let error = WalError::Worker("WAL sync thread stopped".into());
                                    tracing::error!(sequence = completion.0, error = %error);
                                    completion.1.finish(Err(error.clone()));
                                    for append in pending.drain(..) {
                                        append.handle.finish(Err(error.clone()));
                                    }
                                    stop_writer = true;
                                    break;
                                }
                            }
                            if let Err(send_error) = completion_tx.blocking_send(completion) {
                                let error = WalError::Worker("WAL sync thread stopped".into());
                                tracing::error!(sequence = send_error.0.0, error = %error);
                                send_error.0.1.finish(Err(error.clone()));
                                for append in pending.drain(..) {
                                    append.handle.finish(Err(error.clone()));
                                }
                                stop_writer = true;
                                break;
                            }
                        }
                        Err(TrySendError::Closed(completion)) => {
                            let error = WalError::Worker("WAL sync thread stopped".into());
                            tracing::error!(sequence = completion.0, error = %error);
                            completion.1.finish(Err(error.clone()));
                            for append in pending.drain(..) {
                                append.handle.finish(Err(error.clone()));
                            }
                            stop_writer = true;
                            break;
                        }
                    }
                    batch_advanced_sequence = Some(append.sequence);
                }
                if stop_writer {
                    break;
                }
            }

            if let Some(advanced_sequence) = batch_advanced_sequence {
                let active = active_segment.as_ref().map(FileSegment::sync_handle);
                if advanced_tx.send((Some(advanced_sequence), active)).is_err() {
                    let error = WalError::Worker("WAL sync thread stopped".into());
                    tracing::error!(advanced_sequence, error = %error);
                    stop_writer = true;
                }
            }
            if write_then_close || stop_writer {
                break;
            }
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
                        segment.sync_handle().sync()?;
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
        "WAL writer thread panicked".into()
    }
}

#[cfg(test)]
mod tests {
    use super::super::log_syncer::perform_sync;
    use super::*;
    use crate::vfs::StandardVfs;
    use crate::wal::{Log, SegmentLog};
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
                        dirty_segments.push(active_segment.take().unwrap().sync_handle());
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        if let Some(segment) = active_segment.as_ref() {
            dirty_segments.push(segment.sync_handle());
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
