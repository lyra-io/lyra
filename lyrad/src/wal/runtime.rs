use super::error::WalError;
use super::options::WalOptions;
use super::{Lifecycle, Sequence, Wal, WalState};
use crate::segment::{
    AlignedBuffer, FILE_HEADER_SIZE, SegmentFile, SegmentRecord, list_segment_files, sync_directory,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// A batching write-ahead log backed by `lyra`'s segment format.
///
/// Appends are queued into an in-memory channel and drained by a single
/// blocking worker that batches them with `recv_many`, writes them to aligned
/// segment files, and flushes them to stable storage whenever a sync append
/// requires it or the log shuts down. I/O failures are retried until the
/// context is closed, so shutdown is the only thing that stops the worker.
pub struct Log {
    inflight_tx: mpsc::Sender<AppendRequest>,
    context: CancellationToken,
    state: Arc<RwLock<WalState>>,
    tasks: Mutex<Option<JoinSet<()>>>,
}

const RETRY_DELAY: Duration = Duration::from_millis(10);

struct AppendRequest {
    payload: Bytes,
    sync: bool,
    response: oneshot::Sender<Result<Sequence, WalError>>,
}

impl Log {
    /// Opens the WAL at `options.dir`.
    ///
    /// The directory must be empty: segment files from a previous run are not
    /// recovered, so opening a non-empty directory fails with
    /// [`WalError::ExistingSegments`].
    pub async fn open(options: WalOptions) -> Result<Arc<Self>, WalError> {
        options
            .validate()
            .map_err(|message| WalError::InvalidOptions(message.into()))?;
        tokio::fs::create_dir_all(&options.dir).await?;
        if !list_segment_files(&options.dir)?.is_empty() {
            return Err(WalError::ExistingSegments {
                path: options.dir.clone(),
            });
        }

        let state = Arc::new(RwLock::new(WalState::default()));
        let context = CancellationToken::new();
        let runtime = Handle::current();
        let (inflight_tx, inflight_rx) = mpsc::channel(options.queue_capacity);

        let mut tasks = JoinSet::new();
        tasks.spawn_blocking({
            let context = context.clone();
            let runtime = runtime.clone();
            let options = options.clone();
            move || writer_loop(inflight_rx, options, context, runtime)
        });

        Ok(Arc::new(Self {
            inflight_tx,
            context,
            state,
            tasks: Mutex::new(Some(tasks)),
        }))
    }
}

#[async_trait]
impl Wal for Log {
    async fn append(&self, payload: Bytes, sync: bool) -> Result<Sequence, WalError> {
        let state = self.state.read().await;
        if state.lifecycle != Lifecycle::Running {
            return Err(WalError::Closed);
        }

        let (response, receiver) = oneshot::channel();
        self.inflight_tx
            .send(AppendRequest {
                payload,
                sync,
                response,
            })
            .await
            .map_err(|_| WalError::Closed)?;
        drop(state);
        receiver.await.map_err(|_| WalError::Closed)?
    }

    async fn shutdown(&self) -> Result<(), WalError> {
        let mut tasks_guard = self.tasks.lock().await;
        let Some(mut tasks) = tasks_guard.take() else {
            return Ok(());
        };
        drop(tasks_guard);

        {
            let mut state = self.state.write().await;
            state.lifecycle = Lifecycle::Draining;
            self.context.cancel();
        }

        let mut join_error = None;
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result
                && join_error.is_none()
            {
                join_error = Some(WalError::Worker(error.to_string()));
            }
        }
        {
            let mut state = self.state.write().await;
            state.lifecycle = Lifecycle::Closed;
        }
        join_error.map_or(Ok(()), Err)
    }
}

enum WriterEvent {
    Batch(usize),
    Cancelled,
}

fn wait_for_writer_event(
    runtime: &Handle,
    receiver: &mut mpsc::Receiver<AppendRequest>,
    batch: &mut Vec<AppendRequest>,
    max: usize,
    context: &CancellationToken,
) -> WriterEvent {
    runtime.block_on(async {
        tokio::select! {
            _ = context.cancelled() => WriterEvent::Cancelled,
            received = receiver.recv_many(batch, max) => WriterEvent::Batch(received),
        }
    })
}

fn writer_loop(
    mut inflight_rx: mpsc::Receiver<AppendRequest>,
    options: WalOptions,
    context: CancellationToken,
    runtime: Handle,
) {
    let max_batch = options.queue_capacity;
    let mut next_sequence: Sequence = 0;
    let mut next_segment_number: u64 = 1;
    // Active segment bookkeeping: (segment number, file handle, write offset).
    let mut active: Option<(u64, Arc<SegmentFile>, u64)> = None;
    let mut dirty_files: Vec<Arc<SegmentFile>> = Vec::new();
    let mut directory_dirty = false;
    let mut stopping = false;

    loop {
        let mut batch = Vec::new();
        let received = if stopping {
            runtime.block_on(async { inflight_rx.recv_many(&mut batch, max_batch).await })
        } else {
            match wait_for_writer_event(&runtime, &mut inflight_rx, &mut batch, max_batch, &context)
            {
                WriterEvent::Batch(received) => received,
                WriterEvent::Cancelled => {
                    stopping = true;
                    inflight_rx.close();
                    continue;
                }
            }
        };
        if received == 0 {
            break;
        }
        let records = assign_records(&batch, &mut next_sequence);
        if !retry_until(
            || {
                write_records(
                    &records,
                    &mut next_segment_number,
                    &mut active,
                    &mut dirty_files,
                    &mut directory_dirty,
                    &options,
                )
            },
            &context,
            &runtime,
        ) {
            // Shutdown arrived while retrying; drop the batch. Its callers
            // observe Closed because the response channels are dropped.
            continue;
        }

        let mut synced_waiters = Vec::new();
        for (request, (sequence, _)) in batch.into_iter().zip(records) {
            if request.sync {
                synced_waiters.push((sequence, request.response));
            } else {
                let _ = request.response.send(Ok(sequence));
            }
        }
        if synced_waiters.is_empty() {
            continue;
        }

        if !retry_until(
            || perform_sync(&dirty_files, directory_dirty, &options.dir).map_err(WalError::from),
            &context,
            &runtime,
        ) {
            continue;
        }
        dirty_files.clear();
        directory_dirty = false;
        for (sequence, waiter) in synced_waiters {
            let _ = waiter.send(Ok(sequence));
        }
    }

    // Final flush of anything still dirty; all callers have already been
    // answered or dropped, so a failure here is only a lost final flush.
    let _ = perform_sync(&dirty_files, directory_dirty, &options.dir);
}

fn assign_records(batch: &[AppendRequest], next_sequence: &mut Sequence) -> Vec<(Sequence, Bytes)> {
    let mut records = Vec::with_capacity(batch.len());
    for request in batch {
        let sequence = *next_sequence;
        records.push((sequence, request.payload.clone()));
        let incremented = sequence
            .checked_add(1)
            .unwrap_or_else(|| unreachable!("WAL sequence space exhausted"));
        *next_sequence = incremented;
    }
    records
}

fn retry_until<E: std::fmt::Display>(
    mut attempt: impl FnMut() -> Result<(), E>,
    context: &CancellationToken,
    runtime: &Handle,
) -> bool {
    loop {
        if context.is_cancelled() {
            return false;
        }
        match attempt() {
            Ok(()) => return true,
            Err(error) => {
                tracing::warn!(error = %error, "WAL operation failed; it will be retried");
                runtime.block_on(tokio::time::sleep(RETRY_DELAY));
            }
        }
    }
}

fn write_records(
    records: &[(Sequence, Bytes)],
    next_segment_number: &mut u64,
    active: &mut Option<(u64, Arc<SegmentFile>, u64)>,
    dirty_files: &mut Vec<Arc<SegmentFile>>,
    directory_dirty: &mut bool,
    options: &WalOptions,
) -> Result<(), WalError> {
    ensure_active_segment(next_segment_number, active, directory_dirty, options)?;
    let mut encoded = {
        let (number, _, offset) = active.as_ref().unwrap();
        encode_batch(*number, *offset, records)?
    };

    let should_rotate = {
        let (_, _, offset) = active.as_ref().unwrap();
        *offset > FILE_HEADER_SIZE as u64
            && offset.saturating_add(encoded.len() as u64) > options.max_segment_size
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
    options: &WalOptions,
) -> Result<(), WalError> {
    if active.is_some() {
        return Ok(());
    }
    let number = *next_segment_number;
    let file = SegmentFile::create(&options.dir, number, options.io_mode)?;
    *next_segment_number = number
        .checked_add(1)
        .ok_or_else(|| WalError::Worker("segment number exhausted".into()))?;
    *directory_dirty = true;
    *active = Some((number, file, FILE_HEADER_SIZE as u64));
    Ok(())
}

fn encode_batch(
    segment_number: u64,
    start_offset: u64,
    records: &[(Sequence, Bytes)],
) -> Result<Vec<u8>, WalError> {
    let prefixes: Vec<_> = records
        .iter()
        .map(|(sequence, _)| sequence.to_le_bytes())
        .collect();
    let records: Vec<_> = records
        .iter()
        .zip(&prefixes)
        .map(|((_, payload), prefix)| SegmentRecord { prefix, payload })
        .collect();
    crate::segment::encode_batch(segment_number, start_offset, &records).map_err(Into::into)
}

fn perform_sync(files: &[Arc<SegmentFile>], sync_dir: bool, dir: &Path) -> std::io::Result<()> {
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
