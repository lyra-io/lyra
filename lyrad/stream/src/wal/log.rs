//! Stateful write-ahead log implementation.

use super::error::WalError;
use super::ops::{AppendOp, Operation, SyncFile, SyncOp};
use super::options::LogOptions;
use super::publisher::{PublishBatch, PublishTarget, apply_after_sync};
use super::segment::{AppendResult, FileHandle, Segment, list_segment_files, sync_directory};
use super::{
    Lifecycle, Log, MAX_INFLIGHT_APPEND_NUM, MAX_PENDING_PUBLISH_BATCH_NUM, Sequence,
    WAL_SEGMENT_SIZE,
};
use async_trait::async_trait;
use bytes::Bytes;
use meta::utils::directory_lock::DirectoryLock;
use meta::utils::logging::utils::log_ignore;
use std::any::Any;
use std::collections::HashSet;
use std::io::{ErrorKind, Result as IoResult};
use std::path::Path;
use std::sync::Arc;
use std::thread::{Builder as ThreadBuilder, JoinHandle};
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
    recovered_batches: Vec<PublishBatch>,
    active: Option<Segment>,
}

impl SegmentLog {
    /// Opens the WAL at `options.dir`.
    ///
    /// Existing segments are scanned to restore sequence numbering. A valid
    /// final segment remains active for subsequent appends.
    pub async fn open(options: LogOptions) -> Result<Arc<Self>, WalError> {
        Self::open0(options, None).await
    }

    /// Opens the WAL, applies recovered records to `target`, and applies new
    /// batches after their last sequence becomes durable.
    pub async fn open_with_target(
        options: LogOptions,
        target: Arc<dyn PublishTarget>,
    ) -> Result<Arc<Self>, WalError> {
        Self::open0(options, Some(target)).await
    }

    async fn open0(
        options: LogOptions,
        target: Option<Arc<dyn PublishTarget>>,
    ) -> Result<Arc<Self>, WalError> {
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
            let dir = options.dir.clone();
            let last_synced_tx = last_synced_tx.clone();
            move || {
                sync_loop(sync_rx, dir.as_path(), last_synced_tx);
                tracing::info!("WAL sync thread exited");
                let _ = sync_done_tx.send(());
            }
        })?;

        let (writer_started_tx, writer_started) = oneshot::channel();
        let (writer_done_tx, writer_done) = oneshot::channel();
        let recover_records = pending_tx.is_some();
        let applied_offset = target.as_ref().and_then(|target| target.applied_offset());
        let writer_handle = match ThreadBuilder::new().name("lyra-wal-writer".into()).spawn({
            let options = options.clone();
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
                        recover_state(&options, recover_records, applied_offset)
                            .map(|recovered| (directory_lock, recovered))
                    }) {
                    Ok((directory_lock, recovered)) => {
                        let RecoveredState {
                            next_sequence,
                            next_segment_number,
                            recovered_batches,
                            active,
                        } = recovered;
                        let last_sequence = next_sequence.checked_sub(1);
                        if writer_started_tx
                            .send(Ok((last_sequence, recovered_batches)))
                            .is_ok()
                        {
                            writer_loop(
                                operation_rx,
                                sync_tx,
                                pending_tx,
                                options,
                                next_sequence,
                                next_segment_number,
                                active,
                            );
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
                return Err(WalError::Worker(error));
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
    async fn append(&self, payload: Bytes, sync: bool) -> Result<Sequence, WalError> {
        let permit = self
            .operation_tx
            .reserve()
            .await
            .map_err(|_| WalError::Closed)?;
        let state = self.state.read().await;
        if *state != Lifecycle::Running {
            return Err(WalError::Closed);
        }

        let (response, receiver) = oneshot::channel();
        permit.send(Operation::Append(AppendOp {
            payload,
            sync,
            response,
        }));
        drop(state);
        receiver.await.map_err(|_| WalError::Closed)?
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
                .map_err(|panic| WalError::Worker(format!("{name}: {}", panic_message(panic))))
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
    mut operation_rx: mpsc::Receiver<Operation>,
    sync_tx: mpsc::Sender<Operation>,
    pending_publish_tx: Option<mpsc::Sender<PublishBatch>>,
    options: LogOptions,
    mut next_sequence: Sequence,
    mut next_segment_number: u64,
    mut active: Option<Segment>,
) {
    let mut dirty_files = Vec::new();
    let mut directory_dirty = false;

    loop {
        let mut operations = Vec::new();
        let received = operation_rx.blocking_recv_many(&mut operations, MAX_INFLIGHT_APPEND_NUM);
        if received == 0 {
            break;
        }

        let mut batch = Vec::with_capacity(received);
        let mut records = Vec::with_capacity(received);
        let mut batch_error = None;
        let mut write_then_close = false;
        for operation in operations {
            match operation {
                Operation::Append(append_op) => {
                    if batch_error.is_none() {
                        let sequence = next_sequence;
                        if let Some(incremented) = sequence.checked_add(1) {
                            records.push((sequence, append_op.payload.clone()));
                            next_sequence = incremented;
                        } else {
                            batch_error =
                                Some(WalError::Worker("WAL sequence space exhausted".into()));
                        }
                    }
                    batch.push(append_op);
                }
                Operation::Sync(_) => unreachable!("sync operation sent to WAL writer"),
                Operation::Close => {
                    operation_rx.close();
                    write_then_close = true;
                }
            }
        }
        if let Some(error) = batch_error {
            for append_op in batch {
                let _ = append_op.response.send(Err(error.clone()));
            }
            break;
        }
        if batch.is_empty() {
            if write_then_close {
                break;
            }
            continue;
        }

        let mut publish_records = Vec::with_capacity(records.len());
        let write_result = (|| -> Result<(), WalError> {
            for (sequence, payload) in &records {
                let mut record = Vec::with_capacity(8 + payload.len());
                record.extend_from_slice(&sequence.to_le_bytes());
                record.extend_from_slice(payload);

                loop {
                    if active.is_none() {
                        let number = next_segment_number;
                        let incremented = number.checked_add(1).ok_or_else(|| {
                            WalError::Worker("WAL segment number space exhausted".into())
                        })?;
                        active = Some(Segment::create(&options.dir, number, WAL_SEGMENT_SIZE)?);
                        next_segment_number = incremented;
                        directory_dirty = true;
                    }

                    match active.as_mut().unwrap().append(&record)? {
                        AppendResult::Appended(offset) => {
                            let segment = active.as_ref().unwrap();
                            let file = segment.file();
                            let end = segment.write_position();
                            if let Some(candidate) =
                                dirty_files.iter_mut().find(|candidate: &&mut SyncFile| {
                                    Arc::ptr_eq(&candidate.file, &file)
                                })
                            {
                                candidate.end = end;
                            } else {
                                dirty_files.push(SyncFile { file, end });
                            }
                            publish_records.push((
                                *sequence,
                                segment.number(),
                                offset,
                                payload.clone(),
                            ));
                            break;
                        }
                        AppendResult::Full => {
                            active = None;
                        }
                    }
                }
            }
            Ok(())
        })();
        if let Err(error) = write_result {
            tracing::error!(error = %error, "WAL write failed");
            for append_op in batch {
                let _ = append_op.response.send(Err(error.clone()));
            }
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
            && let Err(error) =
                pending_publish_tx.blocking_send(PublishBatch::new(&publish_records))
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
                    let _ = waiter.send(Err(WalError::Closed));
                }
            }
            break;
        }

        if write_then_close {
            break;
        }
    }

    if sync_tx.blocking_send(Operation::Close).is_err() {
        tracing::error!("sync-operation queue closed before the close operation was delivered");
    }
}

fn sync_loop(
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
            match perform_sync(&files, sync_dir, dir).map_err(WalError::from) {
                Ok(()) => {
                    last_synced_sequence.send_replace(Some(last_sequence));
                    for (sequence, waiter) in waiters {
                        let _ = waiter.send(Ok(sequence));
                    }
                }
                Err(error) => {
                    tracing::error!(error = %error, "WAL sync failed");
                    for (_, waiter) in waiters {
                        let _ = waiter.send(Err(error.clone()));
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

/// Scans from the target's last applied record, repairs a truncated final
/// record, and restores the final segment as the active buffered segment.
fn recover_state(
    options: &LogOptions,
    collect_records: bool,
    applied_offset: Option<(u64, u64)>,
) -> Result<RecoveredState, WalError> {
    let mut next_sequence = applied_offset.is_none().then_some(0);
    let mut max_segment_number: u64 = 0;
    let mut recovered_batches = Vec::new();
    let segment_files = list_segment_files(&options.dir)?;
    let segment_count = segment_files.len();
    let mut active = None;
    let mut applied_found = applied_offset.is_none();

    for (index, (file_number, path)) in segment_files.into_iter().enumerate() {
        let final_segment = index + 1 == segment_count;
        max_segment_number = max_segment_number.max(file_number);

        if let Some(offset) = applied_offset
            && !applied_found
            && file_number < offset.0
        {
            continue;
        }
        if let Some(offset) = applied_offset
            && !applied_found
            && file_number > offset.0
        {
            return Err(invalid_applied_offset(options, offset));
        }

        let file = Arc::new(FileHandle::open(&path)?);
        let mut file_size = file.size()?;
        if file_size > WAL_SEGMENT_SIZE {
            return Err(WalError::Corruption {
                path,
                message: format!(
                    "segment size {} exceeds the maximum {WAL_SEGMENT_SIZE}",
                    file_size
                ),
            });
        }

        let starts_at_applied =
            applied_offset.is_some_and(|offset| !applied_found && offset.0 == file_number);
        let start_position = if starts_at_applied {
            applied_offset.unwrap().1
        } else {
            0
        };
        if starts_at_applied && start_position >= file_size {
            return Err(invalid_applied_offset(
                options,
                applied_offset.expect("an applied offset selected the start position"),
            ));
        }

        let segment = Segment::open(Arc::clone(&file), file_number, WAL_SEGMENT_SIZE, file_size)?;
        let mut position = start_position;
        let mut skip_applied_record = !applied_found;
        let mut recovered_records = Vec::new();

        while position < file_size {
            let (next_offset, records) = match segment.read(position, RECOVERY_READ_SIZE) {
                Ok(records) => records,
                Err(WalError::Truncated { .. }) if final_segment && applied_found => {
                    if position < file_size {
                        file.truncate(position)?;
                        file.sync(position)?;
                        file_size = position;
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

            for (record_offset, record) in records {
                let sequence = decode_sequence(&path, &record)?;
                if skip_applied_record {
                    next_sequence =
                        Some(sequence.checked_add(1).ok_or_else(|| {
                            WalError::Worker("WAL sequence space exhausted".into())
                        })?);
                    skip_applied_record = false;
                    applied_found = true;
                    continue;
                }

                let expected_sequence = next_sequence.expect("recovery sequence initialized");
                if sequence != expected_sequence {
                    return Err(WalError::Corruption {
                        path: path.clone(),
                        message: format!(
                            "expected WAL sequence {expected_sequence}, found {sequence}"
                        ),
                    });
                }
                next_sequence = Some(
                    expected_sequence
                        .checked_add(1)
                        .ok_or_else(|| WalError::Worker("WAL sequence space exhausted".into()))?,
                );
                if collect_records {
                    recovered_records.push((
                        sequence,
                        file_number,
                        record_offset,
                        record.slice(8..),
                    ));
                }
            }
            position = next_offset;
        }
        file.discard_cache(position);
        if !recovered_records.is_empty() {
            recovered_batches.push(PublishBatch::new(&recovered_records));
        }
        if final_segment {
            active = Some(Segment::open(
                file,
                file_number,
                WAL_SEGMENT_SIZE,
                file_size,
            )?);
        }
    }

    if let Some(offset) = applied_offset
        && !applied_found
    {
        return Err(invalid_applied_offset(options, offset));
    }

    let next_segment_number = max_segment_number
        .checked_add(1)
        .ok_or_else(|| WalError::Worker("WAL segment number space exhausted".into()))?;
    Ok(RecoveredState {
        // Immutable state
        next_sequence: next_sequence.unwrap_or(0),
        next_segment_number,
        recovered_batches,
        active,
    })
}

fn invalid_applied_offset(options: &LogOptions, offset: (u64, u64)) -> WalError {
    WalError::Corruption {
        path: options.dir.join(format!("{:010}.seg", offset.0)),
        message: format!(
            "applied WAL offset {}:{} does not identify a record",
            offset.0, offset.1
        ),
    }
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

fn perform_sync(files: &[SyncFile], sync_dir: bool, dir: &Path) -> IoResult<()> {
    let mut synced = HashSet::new();
    for sync_file in files.iter().rev() {
        if synced.insert(sync_file.file.path().to_path_buf()) {
            sync_file.file.sync(sync_file.end)?;
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
    use std::fs::OpenOptions;

    fn standard_options(path: &Path) -> LogOptions {
        LogOptions::new(path)
    }

    fn seed_records(
        options: &LogOptions,
        records: &[(Sequence, Bytes)],
        max_records_size: u64,
    ) -> Result<(), WalError> {
        let mut next_segment_number = 1;
        let mut active: Option<Segment> = None;
        let mut dirty_files = Vec::new();

        for (sequence, payload) in records {
            let mut record = Vec::with_capacity(8 + payload.len());
            record.extend_from_slice(&sequence.to_le_bytes());
            record.extend_from_slice(payload);
            loop {
                if active.is_none() {
                    active = Some(Segment::create(
                        &options.dir,
                        next_segment_number,
                        max_records_size,
                    )?);
                    next_segment_number += 1;
                }
                match active.as_mut().unwrap().append(&record)? {
                    AppendResult::Appended(_) => {
                        let segment = active.as_ref().unwrap();
                        let file = segment.file();
                        let end = segment.write_position();
                        if let Some(candidate) = dirty_files
                            .iter_mut()
                            .find(|candidate: &&mut SyncFile| Arc::ptr_eq(&candidate.file, &file))
                        {
                            candidate.end = end;
                        } else {
                            dirty_files.push(SyncFile { file, end });
                        }
                        break;
                    }
                    AppendResult::Full => {
                        active = None;
                    }
                }
            }
        }
        if let Some(segment) = active.as_ref() {
            let file = segment.file();
            let end = segment.write_position();
            if let Some(candidate) = dirty_files
                .iter_mut()
                .find(|candidate: &&mut SyncFile| Arc::ptr_eq(&candidate.file, &file))
            {
                candidate.end = end;
            } else {
                dirty_files.push(SyncFile { file, end });
            }
        }
        perform_sync(&dirty_files, true, &options.dir)?;
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

        assert_eq!(list_segment_files(dir.path()).unwrap().len(), 4);
        let log = SegmentLog::open(options).await.unwrap();
        assert_eq!(
            log.append(Bytes::from_static(b"next"), true).await.unwrap(),
            4
        );
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

        let files = list_segment_files(dir.path()).unwrap();
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

        let path = list_segment_files(dir.path()).unwrap()[0].1.clone();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(151)
            .unwrap();

        let log = SegmentLog::open(options.clone()).await.unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 147);
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

    #[tokio::test]
    async fn recovery_rejects_checksum_corruption_in_the_final_segment() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        seed_segments(&options, &[Bytes::from_static(b"record")], WAL_SEGMENT_SIZE);

        let path = list_segment_files(dir.path()).unwrap()[0].1.clone();
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

        let error = recover_state(&options, false, None).unwrap_err();
        assert!(matches!(
            error,
            WalError::Corruption { message, .. }
                if message == "expected WAL sequence 1, found 2"
        ));
    }

    #[test]
    fn recovery_does_not_read_records_before_the_applied_offset() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        let mut segment = Segment::create(&options.dir, 1, WAL_SEGMENT_SIZE).unwrap();
        let mut offsets = Vec::new();
        for (sequence, payload) in [b"zero".as_slice(), b"one", b"two"].into_iter().enumerate() {
            let mut record = Vec::with_capacity(8 + payload.len());
            record.extend_from_slice(&(sequence as Sequence).to_le_bytes());
            record.extend_from_slice(payload);
            let AppendResult::Appended(offset) = segment.append(&record).unwrap() else {
                panic!("test records must fit in one segment");
            };
            offsets.push(offset);
        }
        segment.file().sync(segment.write_position()).unwrap();

        let path = list_segment_files(dir.path()).unwrap()[0].1.clone();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[11] ^= 0xFF;
        std::fs::write(path, bytes).unwrap();

        let recovered = recover_state(&options, true, Some((1, offsets[1]))).unwrap();
        assert_eq!(recovered.next_sequence, 3);
        let records = recovered
            .recovered_batches
            .iter()
            .flat_map(PublishBatch::records)
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].sequence(), 2);
        assert_eq!(records[0].payload(), &Bytes::from_static(b"two"));
    }

    #[test]
    fn recovery_rejects_a_segment_filename_record_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        seed_segments(&options, &[Bytes::from_static(b"record")], WAL_SEGMENT_SIZE);
        let (_, original_path) = list_segment_files(dir.path()).unwrap().pop().unwrap();
        let renamed_path = dir.path().join("0000000002.seg");
        std::fs::rename(original_path, &renamed_path).unwrap();

        let error = recover_state(&options, false, None).unwrap_err();
        assert!(matches!(
            error,
            WalError::Corruption { path, message }
                if path == renamed_path
                    && message == "physical record segment number mismatch"
        ));
    }
}
