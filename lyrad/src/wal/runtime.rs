use super::error::WalError;
use super::options::WalOptions;
use super::{Sequence, Wal};
use crate::segment::{
    AlignedBuffer, FILE_HEADER_SIZE, SegmentFile, SegmentRecord, list_segment_files, sync_directory,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// A batching write-ahead log backed by `lyra`'s segment format.
///
/// Appends flow through a small pipeline: a blocking writer drains queued
/// appends into batches and writes them to aligned segment files; a blocking
/// syncer flushes files and directories and completes sync appends.
pub struct Log {
    inflight_tx: mpsc::Sender<AppendRequest>,
    context: CancellationToken,
    poison: Arc<Mutex<Option<WalError>>>,
    lifecycle: tokio::sync::RwLock<()>,
    shutdown_lock: tokio::sync::Mutex<()>,
    handles: tokio::sync::Mutex<Option<WorkerHandles>>,
}

struct WorkerHandles {
    writer: JoinHandle<()>,
    syncer: JoinHandle<()>,
}

struct AppendRequest {
    payload: Bytes,
    sync: bool,
    response: oneshot::Sender<Result<Sequence, WalError>>,
}

struct SyncPoint {
    files: Vec<Arc<SegmentFile>>,
    sync_directory: bool,
    waiters: Vec<(Sequence, oneshot::Sender<Result<Sequence, WalError>>)>,
}

struct ActiveSegment {
    number: u64,
    file: Arc<SegmentFile>,
    offset: u64,
}

struct WriterState {
    options: WalOptions,
    next_sequence: Sequence,
    next_segment_number: u64,
    active: Option<ActiveSegment>,
    dirty_files: Vec<Arc<SegmentFile>>,
    directory_dirty: bool,
    poison: Arc<Mutex<Option<WalError>>>,
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
        let poison = Arc::new(Mutex::new(None));
        let context = CancellationToken::new();
        let sync_context = CancellationToken::new();
        let runtime = Handle::current();

        let (inflight_tx, inflight_rx) = mpsc::channel(options.queue_capacity);
        let (sync_tx, sync_rx) = mpsc::channel(8);

        let syncer = tokio::task::spawn_blocking({
            let dir = options.dir.clone();
            let poison = poison.clone();
            let sync_context = sync_context.clone();
            let runtime = runtime.clone();
            move || sync_loop(sync_rx, dir, poison, sync_context, runtime)
        });

        let writer = tokio::task::spawn_blocking({
            let state = WriterState {
                options: options.clone(),
                next_sequence: 0,
                next_segment_number: 1,
                active: None,
                dirty_files: Vec::new(),
                directory_dirty: false,
                poison: poison.clone(),
            };
            let writer_context = context.clone();
            let sync_context = sync_context.clone();
            let runtime = runtime.clone();
            move || {
                writer_loop(
                    inflight_rx,
                    sync_tx,
                    state,
                    writer_context,
                    sync_context,
                    runtime,
                )
            }
        });

        Ok(Arc::new(Self {
            inflight_tx,
            context,
            poison,
            lifecycle: tokio::sync::RwLock::new(()),
            shutdown_lock: tokio::sync::Mutex::new(()),
            handles: tokio::sync::Mutex::new(Some(WorkerHandles { writer, syncer })),
        }))
    }

    fn current_error(&self) -> Option<WalError> {
        self.poison.lock().ok().and_then(|error| error.clone())
    }
}

#[async_trait]
impl Wal for Log {
    async fn append(&self, payload: Bytes, sync: bool) -> Result<Sequence, WalError> {
        let lifecycle = self.lifecycle.read().await;
        if self.context.is_cancelled() {
            return Err(WalError::Closed);
        }
        if let Some(error) = self.current_error() {
            return Err(error);
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
        drop(lifecycle);
        receiver.await.map_err(|_| WalError::Closed)?
    }

    async fn shutdown(&self) -> Result<(), WalError> {
        let _shutdown_guard = self.shutdown_lock.lock().await;
        let mut handles_guard = self.handles.lock().await;
        let Some(handles) = handles_guard.take() else {
            return Ok(());
        };
        drop(handles_guard);

        let lifecycle = self.lifecycle.write().await;
        self.context.cancel();
        drop(lifecycle);

        let mut join_error = None;
        for handle in [handles.writer, handles.syncer] {
            if let Err(error) = handle.await
                && join_error.is_none()
            {
                join_error = Some(WalError::Worker(error.to_string()));
            }
        }
        if let Some(error) = self.current_error() {
            return Err(error);
        }
        join_error.map_or(Ok(()), Err)
    }
}

enum StageEvent<T> {
    Message(Option<T>),
    Cancelled,
}

enum WriterEvent {
    Batch(usize),
    Cancelled,
}

fn wait_for_stage<T>(
    runtime: &Handle,
    receiver: &mut mpsc::Receiver<T>,
    context: &CancellationToken,
) -> StageEvent<T> {
    runtime.block_on(async {
        tokio::select! {
            _ = context.cancelled() => StageEvent::Cancelled,
            message = receiver.recv() => StageEvent::Message(message),
        }
    })
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
    sync_tx: mpsc::Sender<SyncPoint>,
    mut state: WriterState,
    context: CancellationToken,
    sync_context: CancellationToken,
    runtime: Handle,
) {
    let max_batch = state.options.queue_capacity;
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
        state.write_batch(batch, &sync_tx);
    }
    state.finish(&sync_tx);
    sync_context.cancel();
}

impl WriterState {
    fn write_batch(&mut self, batch: Vec<AppendRequest>, sync_tx: &mpsc::Sender<SyncPoint>) {
        if let Some(error) = locked_error(&self.poison) {
            fail_batch(batch, error);
            return;
        }
        if batch.is_empty() {
            return;
        }

        let mut records = Vec::with_capacity(batch.len());
        let mut next = self.next_sequence;
        for request in &batch {
            records.push((next, request.payload.clone()));
            let Some(incremented) = next.checked_add(1) else {
                let error = WalError::Worker("WAL sequence exhausted".into());
                set_poison(&self.poison, error.clone());
                fail_batch(batch, error);
                return;
            };
            next = incremented;
        }

        let write_result = self.write_records(&records);
        if let Err(error) = write_result {
            set_poison(&self.poison, error.clone());
            fail_batch(batch, error);
            return;
        }

        self.next_sequence = next;

        let mut synced_waiters = Vec::new();
        for (request, (sequence, _)) in batch.into_iter().zip(records) {
            if request.sync {
                synced_waiters.push((sequence, request.response));
            } else {
                let _ = request.response.send(Ok(sequence));
            }
        }

        if synced_waiters.is_empty() {
            return;
        }

        let point = self.take_sync_point(synced_waiters);
        if let Err(error) = sync_tx.blocking_send(point) {
            let wal_error = WalError::Worker("WAL sync worker stopped".into());
            fail_waiters(error.0.waiters, wal_error.clone());
            set_poison(&self.poison, wal_error);
        }
    }

    fn write_records(&mut self, records: &[(Sequence, Bytes)]) -> Result<(), WalError> {
        self.ensure_active_segment()?;
        let mut encoded = {
            let active = self.active.as_ref().unwrap();
            encode_batch(active.number, active.offset, records)?
        };

        let should_rotate = {
            let active = self.active.as_ref().unwrap();
            active.offset > FILE_HEADER_SIZE as u64
                && active.offset.saturating_add(encoded.len() as u64)
                    > self.options.max_segment_size
        };
        if should_rotate {
            self.active = None;
            self.ensure_active_segment()?;
            let active = self.active.as_ref().unwrap();
            encoded = encode_batch(active.number, active.offset, records)?;
        }

        let buffer = AlignedBuffer::from_slice(&encoded);
        let active = self.active.as_mut().unwrap();
        active.file.write_aligned(&buffer, active.offset)?;
        active.offset += encoded.len() as u64;
        if !self
            .dirty_files
            .iter()
            .any(|file| Arc::ptr_eq(file, &active.file))
        {
            self.dirty_files.push(active.file.clone());
        }
        Ok(())
    }

    fn ensure_active_segment(&mut self) -> Result<(), WalError> {
        if self.active.is_some() {
            return Ok(());
        }
        let number = self.next_segment_number;
        let file = SegmentFile::create(&self.options.dir, number, self.options.io_mode)?;
        self.next_segment_number = number
            .checked_add(1)
            .ok_or_else(|| WalError::Worker("segment number exhausted".into()))?;
        self.directory_dirty = true;
        self.active = Some(ActiveSegment {
            number,
            file,
            offset: FILE_HEADER_SIZE as u64,
        });
        Ok(())
    }

    fn take_sync_point(
        &mut self,
        waiters: Vec<(Sequence, oneshot::Sender<Result<Sequence, WalError>>)>,
    ) -> SyncPoint {
        SyncPoint {
            files: std::mem::take(&mut self.dirty_files),
            sync_directory: std::mem::take(&mut self.directory_dirty),
            waiters,
        }
    }

    fn finish(&mut self, sync_tx: &mpsc::Sender<SyncPoint>) {
        let point = self.take_sync_point(Vec::new());
        if let Err(error) = sync_tx.blocking_send(point) {
            let wal_error = WalError::Worker("WAL sync worker stopped".into());
            fail_waiters(error.0.waiters, wal_error.clone());
            set_poison(&self.poison, wal_error);
        }
    }
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

fn sync_loop(
    mut sync_rx: mpsc::Receiver<SyncPoint>,
    dir: PathBuf,
    poison: Arc<Mutex<Option<WalError>>>,
    context: CancellationToken,
    runtime: Handle,
) {
    let mut stopping = false;
    loop {
        let mut point = if stopping {
            match sync_rx.try_recv() {
                Ok(point) => point,
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        } else {
            match wait_for_stage(&runtime, &mut sync_rx, &context) {
                StageEvent::Message(Some(point)) => point,
                StageEvent::Message(None) => break,
                StageEvent::Cancelled => {
                    stopping = true;
                    sync_rx.close();
                    continue;
                }
            }
        };
        loop {
            let Ok(next) = sync_rx.try_recv() else {
                break;
            };
            merge_sync_points(&mut point, next);
        }

        let result = if let Some(error) = locked_error(&poison) {
            Err(error)
        } else {
            perform_sync(&point, &dir).map_err(WalError::from)
        };

        match result {
            Ok(()) => {
                for (sequence, waiter) in point.waiters {
                    let _ = waiter.send(Ok(sequence));
                }
            }
            Err(error) => {
                set_poison(&poison, error.clone());
                fail_waiters(point.waiters, error);
            }
        }
    }
}

fn perform_sync(point: &SyncPoint, dir: &std::path::Path) -> std::io::Result<()> {
    let mut synced = HashSet::new();
    for file in &point.files {
        if synced.insert(file.path().to_path_buf()) {
            file.sync_data()?;
        }
    }
    if point.sync_directory {
        sync_directory(dir)?;
    }
    Ok(())
}

fn merge_sync_points(target: &mut SyncPoint, mut next: SyncPoint) {
    target.files.append(&mut next.files);
    target.sync_directory |= next.sync_directory;
    target.waiters.append(&mut next.waiters);
}

fn fail_batch(batch: Vec<AppendRequest>, error: WalError) {
    for request in batch {
        let _ = request.response.send(Err(error.clone()));
    }
}

fn fail_waiters(
    waiters: Vec<(Sequence, oneshot::Sender<Result<Sequence, WalError>>)>,
    error: WalError,
) {
    for (_, waiter) in waiters {
        let _ = waiter.send(Err(error.clone()));
    }
}

fn locked_error(poison: &Mutex<Option<WalError>>) -> Option<WalError> {
    poison.lock().ok().and_then(|error| error.clone())
}

fn set_poison(poison: &Mutex<Option<WalError>>, error: WalError) {
    if let Ok(mut current) = poison.lock()
        && current.is_none()
    {
        *current = Some(error);
    }
}
