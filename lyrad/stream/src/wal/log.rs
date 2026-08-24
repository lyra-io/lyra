//! Stateful write-ahead log implementation.

use super::error::LogError;
use super::ops::{AppendOp, Operation, SyncOp};
use super::options::LogOptions;
use super::publisher::{PublishBatch, PublishTarget, apply_after_sync};
use super::{
    Lifecycle, Log, MAX_INFLIGHT_APPEND_NUM, MAX_PENDING_PUBLISH_BATCH_NUM, Sequence,
    WAL_SEGMENT_SIZE,
};
#[cfg(test)]
use crate::segment::IoMode;
use crate::segment::{
    AlignedBuffer, FILE_HEADER_SIZE, SegmentFile, SegmentRecord,
    encode_batch as encode_segment_batch, list_segment_files, scan_segment, sync_directory,
};
use async_trait::async_trait;
use bytes::Bytes;
use meta::utils::directory_lock::DirectoryLock;
use meta::utils::logging::utils::log_ignore;
use std::any::Any;
use std::collections::HashSet;
use std::fmt::Display;
use std::fs::OpenOptions;
use std::io::{ErrorKind, Result as IoResult};
use std::path::Path;
use std::sync::Arc;
use std::thread::{self, Builder as ThreadBuilder, JoinHandle};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// A batching write-ahead log backed by Lyra's segment format.
///
/// A dedicated writer thread assigns sequences and writes batches. A second
/// dedicated thread groups synchronization work and advances the highest
/// durable sequence through a watch channel. When a [`PublishTarget`] is
/// configured, a Tokio task forwards batches only after their last sequence
/// is durable.
pub struct SegmentLog {
    // Control state
    context: CancellationToken,
    workers: Mutex<Option<WorkerThreads>>,
    tasks: Mutex<Option<JoinSet<()>>>,
    close_lock: Mutex<()>,

    // Immutable state
    operation_tx: mpsc::Sender<Operation>,
    target: Option<Arc<dyn PublishTarget>>,

    // Mutable state
    state: RwLock<Lifecycle>,
}

const RETRY_DELAY: Duration = Duration::from_millis(10);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(30);
const LOCK_FILE_NAME: &str = ".lyra-wal.lock";

struct WorkerThreads {
    writer: WorkerThread,
    sync: WorkerThread,
}

struct WorkerThread {
    // Control state
    handle: Option<JoinHandle<()>>,
    done: oneshot::Receiver<()>,
}

fn lock_directory(dir: &Path) -> Result<DirectoryLock, LogError> {
    match DirectoryLock::acquire(dir.join(LOCK_FILE_NAME)) {
        Ok(lock) => Ok(lock),
        Err(error) if error.kind() == ErrorKind::WouldBlock => {
            Err(LogError::Locked(dir.to_path_buf()))
        }
        Err(error) => Err(error.into()),
    }
}

impl SegmentLog {
    /// Opens the WAL at `options.dir`.
    ///
    /// Existing segment files from a previous run are scanned and recovered:
    /// sequence numbering resumes after the last durable record, and new
    /// appends continue in a fresh segment.
    pub async fn open(options: LogOptions) -> Result<Arc<Self>, LogError> {
        Self::open0(options, None).await
    }

    /// Opens the WAL, applies recovered records to `target`, and applies new
    /// batches after their last sequence becomes durable.
    pub async fn open_with_target(
        options: LogOptions,
        target: Arc<dyn PublishTarget>,
    ) -> Result<Arc<Self>, LogError> {
        Self::open0(options, Some(target)).await
    }

    async fn open0(
        options: LogOptions,
        target: Option<Arc<dyn PublishTarget>>,
    ) -> Result<Arc<Self>, LogError> {
        tokio::fs::create_dir_all(&options.dir).await?;

        let context = CancellationToken::new();
        let (operation_tx, operation_rx) = mpsc::channel(MAX_INFLIGHT_APPEND_NUM);
        let (sync_tx, sync_rx) = mpsc::channel(MAX_INFLIGHT_APPEND_NUM);
        let (last_synced_tx, last_synced_sequence) = watch::channel(None);
        let (pending_tx, pending_rx) = if target.is_some() {
            let (tx, rx) = mpsc::channel(MAX_PENDING_PUBLISH_BATCH_NUM);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let (sync_done_tx, sync_done) = oneshot::channel();
        let sync_handle = ThreadBuilder::new().name("lyra-wal-sync".into()).spawn({
            let context = context.clone();
            let dir = options.dir.clone();
            let last_synced_tx = last_synced_tx.clone();
            move || {
                sync_loop(context, sync_rx, dir.as_path(), last_synced_tx);
                let _ = sync_done_tx.send(());
            }
        })?;

        let (writer_started_tx, writer_started) = oneshot::channel();
        let (writer_done_tx, writer_done) = oneshot::channel();
        let recover_records = pending_tx.is_some();
        let writer_handle = match ThreadBuilder::new().name("lyra-wal-writer".into()).spawn({
            let context = context.clone();
            let options = options.clone();
            move || {
                match lock_directory(&options.dir).and_then(|directory_lock| {
                    recover_state(&options.dir, recover_records)
                        .map(|recovered| (directory_lock, recovered))
                }) {
                    Ok((
                        directory_lock,
                        (next_sequence, next_segment_number, recovered_batches),
                    )) => {
                        let last_sequence = next_sequence.checked_sub(1);
                        if writer_started_tx
                            .send(Ok((last_sequence, recovered_batches)))
                            .is_ok()
                        {
                            writer_loop(
                                context,
                                operation_rx,
                                sync_tx,
                                pending_tx,
                                options,
                                next_sequence,
                                next_segment_number,
                            );
                        }
                        drop(directory_lock);
                    }
                    Err(error) => {
                        let _ = writer_started_tx.send(Err(error));
                    }
                }
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

        let recovered_batches = match writer_started.await {
            Ok(Ok((last_sequence, recovered_batches))) => {
                last_synced_tx.send_replace(last_sequence);
                recovered_batches
            }
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
                return Err(LogError::Worker(error));
            }
        };

        if let Some(target) = &target {
            for batch in recovered_batches {
                if let Err(error) = target.apply(batch).await {
                    context.cancel();
                    let _ = operation_tx.send(Operation::Close).await;
                    let _ = writer_done.await;
                    let _ = sync_done.await;
                    let _ = writer_handle.join();
                    let _ = sync_handle.join();
                    return Err(error);
                }
            }
        }

        let mut tasks = JoinSet::new();
        if let (Some(target), Some(pending_rx)) = (target.clone(), pending_rx) {
            tasks.spawn(apply_after_sync(
                context.clone(),
                pending_rx,
                last_synced_sequence.clone(),
                target,
            ));
        }

        Ok(Arc::new(Self {
            context,
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
            tasks: Mutex::new(Some(tasks)),
            close_lock: Mutex::new(()),
            operation_tx,
            target,
            state: RwLock::new(Lifecycle::Running),
        }))
    }
}

#[async_trait]
impl Log for SegmentLog {
    /// Appends `payload` and returns its assigned sequence.
    ///
    /// A sync append is acknowledged after its sequence becomes durable. A
    /// non-sync append is acknowledged after its bytes are written; the sync
    /// thread still makes the batch durable in the background.
    async fn append(&self, payload: Bytes, sync: bool) -> Result<Sequence, LogError> {
        let permit = self
            .operation_tx
            .reserve()
            .await
            .map_err(|_| LogError::Closed)?;
        let state = self.state.read().await;
        if *state != Lifecycle::Running {
            return Err(LogError::Closed);
        }

        let (response, receiver) = oneshot::channel();
        permit.send(Operation::Append(AppendOp {
            payload,
            sync,
            response,
        }));
        drop(state);
        receiver.await.map_err(|_| LogError::Closed)?
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

        self.context.cancel();
        log_ignore!(
            "send-writer-close",
            self.operation_tx
                .send(Operation::Close)
                .await
                .map_err(|_| LogError::Closed)
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

        close_tasks(&self.tasks).await;
        if let Some(target) = &self.target {
            match tokio::time::timeout(CLOSE_TIMEOUT, target.close()).await {
                Ok(result) => log_ignore!("close-publish-target", result),
                Err(error) => log_ignore!("close-publish-target-timeout", Err::<(), _>(error)),
            }
        }

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
                .map_err(|panic| LogError::Worker(format!("{name}: {}", panic_message(panic))))
        );
    } else {
        drop(handle);
    }
}

async fn close_tasks(tasks: &Mutex<Option<JoinSet<()>>>) {
    let mut tasks_guard = tasks.lock().await;
    let Some(tasks) = tasks_guard.as_mut() else {
        return;
    };
    let drain = async {
        while let Some(result) = tasks.join_next().await {
            log_ignore!("join-background-task", result);
        }
    };
    if let Err(error) = tokio::time::timeout(CLOSE_TIMEOUT, drain).await {
        tracing::error!(error = %error, "background task close timed out; aborting tasks");
        tasks.abort_all();
        while let Some(result) = tasks.join_next().await {
            log_ignore!("join-aborted-background-task", result);
        }
    }
    tasks_guard.take();
}

fn writer_loop(
    context: CancellationToken,
    mut operation_rx: mpsc::Receiver<Operation>,
    sync_tx: mpsc::Sender<Operation>,
    pending_publish_tx: Option<mpsc::Sender<PublishBatch>>,
    options: LogOptions,
    mut next_sequence: Sequence,
    mut next_segment_number: u64,
) {
    let max_batch = MAX_INFLIGHT_APPEND_NUM;
    // Active segment bookkeeping: (segment number, file handle, write offset).
    let mut active: Option<(u64, Arc<SegmentFile>, u64)> = None;
    let mut dirty_files = Vec::new();
    let mut directory_dirty = false;

    loop {
        let mut operations = Vec::new();
        let received = operation_rx.blocking_recv_many(&mut operations, max_batch);
        if received == 0 {
            break;
        }

        let mut batch = Vec::new();
        let mut closing = false;
        for operation in operations {
            match operation {
                Operation::Append(request) => batch.push(request),
                Operation::Sync(_) => unreachable!("sync operation sent to WAL writer"),
                Operation::Close => {
                    operation_rx.close();
                    closing = true;
                }
            }
        }
        if batch.is_empty() {
            if closing {
                break;
            }
            continue;
        }

        let records = match assign_records(&batch, &mut next_sequence) {
            Ok(records) => records,
            Err(error) => {
                respond_to_batch(batch, error);
                break;
            }
        };
        if let Err(error) = retry_until_cancelled(&context, || {
            write_records(
                &records,
                &mut next_segment_number,
                &mut active,
                &mut dirty_files,
                &mut directory_dirty,
                &options,
            )
        }) {
            tracing::error!(error = %error, "WAL write failed during close and was ignored");
            respond_to_batch(batch, LogError::Closed);
            break;
        }

        let mut sync_waiters = Vec::new();
        for (request, (sequence, _)) in batch.into_iter().zip(&records) {
            if request.sync {
                sync_waiters.push((*sequence, request.response));
            } else {
                let _ = request.response.send(Ok(*sequence));
            }
        }

        if let Some(pending_publish_tx) = &pending_publish_tx
            && let Err(error) = pending_publish_tx.blocking_send(PublishBatch::new(&records))
        {
            tracing::error!(error = %error, "pending-publish queue closed; batch was not published");
        }

        let sync_op = SyncOp {
            files: std::mem::take(&mut dirty_files),
            sync_directory: std::mem::take(&mut directory_dirty),
            last_sequence: records.last().unwrap().0,
            waiters: sync_waiters,
        };
        if let Err(error) = sync_tx.blocking_send(Operation::Sync(sync_op)) {
            tracing::error!("sync-operation queue closed before a batch was delivered");
            if let Operation::Sync(sync_op) = error.0 {
                for (_, waiter) in sync_op.waiters {
                    let _ = waiter.send(Err(LogError::Closed));
                }
            }
            break;
        }

        if closing {
            break;
        }
    }

    if sync_tx.blocking_send(Operation::Close).is_err() {
        tracing::error!("sync-operation queue closed before the close operation was delivered");
    }
}

fn sync_loop(
    context: CancellationToken,
    mut sync_rx: mpsc::Receiver<Operation>,
    dir: &Path,
    last_synced_sequence: watch::Sender<Option<Sequence>>,
) {
    loop {
        let mut operations = Vec::new();
        let received = sync_rx.blocking_recv_many(&mut operations, MAX_INFLIGHT_APPEND_NUM);
        if received == 0 {
            break;
        }

        let mut files = Vec::new();
        let mut sync_dir = false;
        let mut last_sequence = None;
        let mut waiters = Vec::new();
        let mut closing = false;
        for operation in operations {
            match operation {
                Operation::Sync(sync_op) => {
                    files.extend(sync_op.files);
                    sync_dir |= sync_op.sync_directory;
                    last_sequence = Some(sync_op.last_sequence);
                    waiters.extend(sync_op.waiters);
                }
                Operation::Append(_) => unreachable!("append operation sent to WAL sync worker"),
                Operation::Close => {
                    sync_rx.close();
                    closing = true;
                }
            }
        }

        if let Some(last_sequence) = last_sequence {
            match retry_until_cancelled(&context, || {
                perform_sync(&files, sync_dir, dir).map_err(LogError::from)
            }) {
                Ok(()) => {
                    last_synced_sequence.send_replace(Some(last_sequence));
                    for (sequence, waiter) in waiters {
                        let _ = waiter.send(Ok(sequence));
                    }
                }
                Err(error) => {
                    tracing::error!(error = %error, "WAL sync failed during close and was ignored");
                    for (_, waiter) in waiters {
                        let _ = waiter.send(Err(error.clone()));
                    }
                }
            }
        }

        if closing {
            break;
        }
    }
}

fn respond_to_batch(batch: Vec<AppendOp>, error: LogError) {
    for request in batch {
        let _ = request.response.send(Err(error.clone()));
    }
}

/// Scans existing segments and returns the next sequence, next segment number,
/// and optional durable batches to apply.
fn recover_state(
    dir: &Path,
    collect_records: bool,
) -> Result<(Sequence, u64, Vec<PublishBatch>), LogError> {
    let mut next_sequence: Sequence = 0;
    let mut max_segment_number: u64 = 0;
    let mut recovered_batches = Vec::new();
    let segment_files = list_segment_files(dir)?;
    let last_index = segment_files.len().saturating_sub(1);
    for (index, (file_number, path)) in segment_files.into_iter().enumerate() {
        let scan = scan_segment(&path, index == last_index)?;
        validate_segment_number(file_number, scan.segment_number, &path)?;
        if index == last_index {
            truncate_torn_tail(&path, scan.valid_len)?;
        }
        max_segment_number = max_segment_number.max(file_number);
        let mut recovered_records = Vec::new();
        for record in scan.records {
            let sequence = decode_sequence(&path, &record)?;
            if sequence != next_sequence {
                return Err(LogError::Corruption {
                    path,
                    message: format!("expected WAL sequence {next_sequence}, found {sequence}"),
                });
            }
            next_sequence = next_sequence
                .checked_add(1)
                .ok_or_else(|| LogError::Worker("WAL sequence space exhausted".into()))?;
            if collect_records {
                recovered_records.push((sequence, record.slice(8..)));
            }
        }
        if !recovered_records.is_empty() {
            recovered_batches.push(PublishBatch::new(&recovered_records));
        }
    }
    let next_segment_number = max_segment_number
        .checked_add(1)
        .ok_or_else(|| LogError::Worker("WAL segment number space exhausted".into()))?;
    Ok((next_sequence, next_segment_number, recovered_batches))
}

fn truncate_torn_tail(path: &Path, valid_len: u64) -> Result<(), LogError> {
    if std::fs::metadata(path)?.len() == valid_len {
        return Ok(());
    }
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(valid_len)?;
    file.sync_data()?;
    Ok(())
}

fn validate_segment_number(
    file_number: u64,
    header_number: u64,
    path: &Path,
) -> Result<(), LogError> {
    if file_number == header_number {
        return Ok(());
    }
    Err(LogError::Corruption {
        path: path.to_path_buf(),
        message: format!(
            "segment filename identifies {file_number}, but its header identifies {header_number}"
        ),
    })
}

fn decode_sequence(path: &Path, record: &[u8]) -> Result<Sequence, LogError> {
    let prefix = record.get(..8).ok_or_else(|| LogError::Corruption {
        path: path.to_path_buf(),
        message: "record shorter than the sequence prefix".into(),
    })?;
    Ok(u64::from_le_bytes(prefix.try_into().unwrap()))
}

fn assign_records(
    batch: &[AppendOp],
    next_sequence: &mut Sequence,
) -> Result<Vec<(Sequence, Bytes)>, LogError> {
    let mut records = Vec::with_capacity(batch.len());
    for request in batch {
        let sequence = *next_sequence;
        records.push((sequence, request.payload.clone()));
        let incremented = sequence
            .checked_add(1)
            .ok_or_else(|| LogError::Worker("WAL sequence space exhausted".into()))?;
        *next_sequence = incremented;
    }
    Ok(records)
}

fn retry_until_cancelled<E: Display>(
    context: &CancellationToken,
    mut attempt: impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    loop {
        match attempt() {
            Ok(()) => return Ok(()),
            Err(error) if context.is_cancelled() => return Err(error),
            Err(error) => {
                tracing::warn!(error = %error, "WAL operation failed; it will be retried");
                thread::sleep(RETRY_DELAY);
            }
        }
    }
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

fn write_records(
    records: &[(Sequence, Bytes)],
    next_segment_number: &mut u64,
    active: &mut Option<(u64, Arc<SegmentFile>, u64)>,
    dirty_files: &mut Vec<Arc<SegmentFile>>,
    directory_dirty: &mut bool,
    options: &LogOptions,
) -> Result<(), LogError> {
    write_records_with_segment_size(
        records,
        next_segment_number,
        active,
        dirty_files,
        directory_dirty,
        options,
        WAL_SEGMENT_SIZE,
    )
}

#[allow(clippy::too_many_arguments)]
fn write_records_with_segment_size(
    records: &[(Sequence, Bytes)],
    next_segment_number: &mut u64,
    active: &mut Option<(u64, Arc<SegmentFile>, u64)>,
    dirty_files: &mut Vec<Arc<SegmentFile>>,
    directory_dirty: &mut bool,
    options: &LogOptions,
    segment_size: u64,
) -> Result<(), LogError> {
    ensure_active_segment(next_segment_number, active, directory_dirty, options)?;
    let mut encoded = {
        let (number, _, offset) = active.as_ref().unwrap();
        encode_batch(*number, *offset, records)?
    };

    let should_rotate = {
        let (_, _, offset) = active.as_ref().unwrap();
        *offset > FILE_HEADER_SIZE as u64
            && offset.saturating_add(encoded.len() as u64) > segment_size
    };
    if should_rotate {
        *active = None;
        ensure_active_segment(next_segment_number, active, directory_dirty, options)?;
        let (number, _, offset) = active.as_ref().unwrap();
        encoded = encode_batch(*number, *offset, records)?;
    }

    let buffer = AlignedBuffer::from_slice(&encoded);
    let (_, file, offset) = active.as_mut().unwrap();
    file.write_aligned(&buffer, *offset)?;
    *offset += encoded.len() as u64;
    if !dirty_files
        .iter()
        .any(|candidate| Arc::ptr_eq(candidate, file))
    {
        dirty_files.push(file.clone());
    }
    Ok(())
}

fn ensure_active_segment(
    next_segment_number: &mut u64,
    active: &mut Option<(u64, Arc<SegmentFile>, u64)>,
    directory_dirty: &mut bool,
    options: &LogOptions,
) -> Result<(), LogError> {
    if active.is_some() {
        return Ok(());
    }
    let number = *next_segment_number;
    let file = SegmentFile::create(&options.dir, number, options.io_mode)?;
    *next_segment_number = number
        .checked_add(1)
        .ok_or_else(|| LogError::Worker("segment number exhausted".into()))?;
    *directory_dirty = true;
    *active = Some((number, file, FILE_HEADER_SIZE as u64));
    Ok(())
}

fn encode_batch(
    segment_number: u64,
    start_offset: u64,
    records: &[(Sequence, Bytes)],
) -> Result<Vec<u8>, LogError> {
    let prefixes: Vec<_> = records
        .iter()
        .map(|(sequence, _)| sequence.to_le_bytes())
        .collect();
    let records: Vec<_> = records
        .iter()
        .zip(&prefixes)
        .map(|((_, payload), prefix)| SegmentRecord { prefix, payload })
        .collect();
    encode_segment_batch(segment_number, start_offset, &records).map_err(Into::into)
}

fn perform_sync(files: &[Arc<SegmentFile>], sync_dir: bool, dir: &Path) -> IoResult<()> {
    let mut synced = HashSet::new();
    for file in files {
        if synced.insert(file.path().to_path_buf()) {
            file.sync_data()?;
        }
    }
    if sync_dir {
        sync_directory(dir)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn standard_options(path: &Path) -> LogOptions {
        let mut options = LogOptions::new(path);
        options.io_mode = IoMode::Standard;
        options
    }

    fn seed_segments(options: &LogOptions, payloads: &[Bytes], segment_size: u64) {
        let mut next_segment_number = 1;
        let mut active = None;
        let mut dirty_files = Vec::new();
        let mut directory_dirty = false;

        for (sequence, payload) in payloads.iter().enumerate() {
            write_records_with_segment_size(
                &[(sequence as Sequence, payload.clone())],
                &mut next_segment_number,
                &mut active,
                &mut dirty_files,
                &mut directory_dirty,
                options,
                segment_size,
            )
            .unwrap();
        }
        perform_sync(&dirty_files, directory_dirty, &options.dir).unwrap();
    }

    #[tokio::test]
    async fn recovers_sequence_across_rotated_segments() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        let payloads: Vec<_> = (0..4u8)
            .map(|value| Bytes::from(vec![value; 128]))
            .collect();
        seed_segments(&options, &payloads, 8192);

        assert_eq!(list_segment_files(dir.path()).unwrap().len(), 4);
        let log = SegmentLog::open(options).await.unwrap();
        assert_eq!(
            log.append(Bytes::from_static(b"next"), true).await.unwrap(),
            4
        );
        log.close().await;
    }

    #[test]
    fn a_record_may_exceed_the_segment_size() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        let payload = Bytes::from(vec![0xA5; 1024 * 1024 + 17]);
        seed_segments(&options, std::slice::from_ref(&payload), 64 * 1024);

        let files = list_segment_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        let scan = scan_segment(&files[0].1, false).unwrap();
        assert_eq!(&scan.records[0][8..], payload);
    }

    #[tokio::test]
    async fn recovery_rejects_a_torn_nonfinal_segment() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        let payloads = vec![Bytes::from(vec![0x11; 128]), Bytes::from(vec![0x22; 128])];
        seed_segments(&options, &payloads, 8192);

        let files = list_segment_files(dir.path()).unwrap();
        assert_eq!(files.len(), 2);
        OpenOptions::new()
            .write(true)
            .open(&files[0].1)
            .unwrap()
            .set_len(FILE_HEADER_SIZE as u64 + 4)
            .unwrap();

        let error = SegmentLog::open(options).await.err().unwrap();
        assert!(matches!(error, LogError::Corruption { .. }));
    }

    #[tokio::test]
    async fn recovery_truncates_a_torn_final_segment_before_continuing() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        let payloads = vec![Bytes::from(vec![0x11; 128]), Bytes::from(vec![0x22; 128])];
        seed_segments(&options, &payloads, WAL_SEGMENT_SIZE);

        let path = list_segment_files(dir.path()).unwrap()[0].1.clone();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len((FILE_HEADER_SIZE * 2 + 4) as u64)
            .unwrap();

        let log = SegmentLog::open(options.clone()).await.unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 8192);
        assert_eq!(
            log.append(Bytes::from_static(b"replacement"), true)
                .await
                .unwrap(),
            1
        );
        log.close().await;

        let reopened = SegmentLog::open(options).await.unwrap();
        assert_eq!(
            reopened
                .append(Bytes::from_static(b"next"), true)
                .await
                .unwrap(),
            2
        );
        reopened.close().await;
    }

    #[test]
    fn recovery_rejects_a_sequence_gap() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        let mut next_segment_number = 1;
        let mut active = None;
        let mut dirty_files = Vec::new();
        let mut directory_dirty = false;
        let records = [
            (0, Bytes::from_static(b"zero")),
            (2, Bytes::from_static(b"two")),
        ];

        write_records_with_segment_size(
            &records,
            &mut next_segment_number,
            &mut active,
            &mut dirty_files,
            &mut directory_dirty,
            &options,
            WAL_SEGMENT_SIZE,
        )
        .unwrap();
        perform_sync(&dirty_files, directory_dirty, &options.dir).unwrap();

        let error = recover_state(&options.dir, false).unwrap_err();
        assert!(matches!(
            error,
            LogError::Corruption { message, .. }
                if message == "expected WAL sequence 1, found 2"
        ));
    }

    #[test]
    fn recovery_rejects_a_segment_filename_header_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        seed_segments(&options, &[Bytes::from_static(b"record")], WAL_SEGMENT_SIZE);
        let (_, original_path) = list_segment_files(dir.path()).unwrap().pop().unwrap();
        let renamed_path = dir.path().join("0000000002.seg");
        std::fs::rename(original_path, &renamed_path).unwrap();

        let error = recover_state(&options.dir, false).unwrap_err();
        assert!(matches!(
            error,
            LogError::Corruption { path, message }
                if path == renamed_path
                    && message == "segment filename identifies 2, but its header identifies 1"
        ));
    }
}
