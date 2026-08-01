use super::error::WalError;
use super::format::{FILE_HEADER_SIZE, encode_batch};
use super::io::{AlignedBuffer, SegmentFile, list_segment_files, sync_directory};
use super::options::WalOptions;
use super::reader::{SegmentWalReader, recover_directory};
use super::{Sequence, Wal};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

const NO_SEQUENCE: u64 = u64::MAX;

pub struct SegmentWal {
    options: WalOptions,
    ingress_tx: mpsc::Sender<IngressCommand>,
    durable_sequence: Arc<AtomicU64>,
    earliest_sequence: Arc<AtomicU64>,
    poison: Arc<Mutex<Option<WalError>>>,
    closed: AtomicBool,
    lifecycle: tokio::sync::RwLock<()>,
    shutdown_lock: tokio::sync::Mutex<()>,
    handles: tokio::sync::Mutex<Option<WorkerHandles>>,
}

struct WorkerHandles {
    batcher: JoinHandle<()>,
    writer: JoinHandle<()>,
    syncer: JoinHandle<()>,
}

struct AppendRequest {
    payload: Bytes,
    sync: bool,
    response: oneshot::Sender<Result<Sequence, WalError>>,
}

enum IngressCommand {
    Append(AppendRequest),
    Shutdown(oneshot::Sender<Result<(), WalError>>),
}

enum WriterCommand {
    Batch(Vec<AppendRequest>),
    Shutdown(oneshot::Sender<Result<(), WalError>>),
}

struct SyncPoint {
    files: Vec<Arc<SegmentFile>>,
    sync_directory: bool,
    through_sequence: Option<Sequence>,
    waiters: Vec<(Sequence, oneshot::Sender<Result<Sequence, WalError>>)>,
}

enum SyncCommand {
    Sync(SyncPoint),
    Shutdown {
        point: SyncPoint,
        response: oneshot::Sender<Result<(), WalError>>,
    },
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
    last_written_sequence: Option<Sequence>,
    active: Option<ActiveSegment>,
    dirty_files: Vec<Arc<SegmentFile>>,
    directory_dirty: bool,
    earliest_sequence: Arc<AtomicU64>,
    poison: Arc<Mutex<Option<WalError>>>,
}

impl SegmentWal {
    pub async fn open(options: WalOptions) -> Result<Arc<Self>, WalError> {
        options
            .validate()
            .map_err(|message| WalError::InvalidOptions(message.into()))?;
        tokio::fs::create_dir_all(&options.dir).await?;

        let recovery_dir = options.dir.clone();
        let recovery = tokio::task::spawn_blocking(move || recover_directory(&recovery_dir))
            .await
            .map_err(|error| WalError::Worker(error.to_string()))??;
        let next_sequence = match recovery.last_sequence {
            Some(sequence) => sequence
                .checked_add(1)
                .ok_or_else(|| WalError::Worker("WAL sequence exhausted".into()))?,
            None => 0,
        };
        let durable = recovery.last_sequence.unwrap_or(NO_SEQUENCE);
        let earliest = recovery.earliest_sequence.unwrap_or(NO_SEQUENCE);

        let durable_sequence = Arc::new(AtomicU64::new(durable));
        let earliest_sequence = Arc::new(AtomicU64::new(earliest));
        let poison = Arc::new(Mutex::new(None));

        let (ingress_tx, ingress_rx) = mpsc::channel(options.queue_capacity);
        let (writer_tx, writer_rx) = mpsc::channel(8);
        let (sync_tx, sync_rx) = mpsc::channel(8);

        let syncer = tokio::task::spawn_blocking({
            let dir = options.dir.clone();
            let durable_sequence = durable_sequence.clone();
            let poison = poison.clone();
            move || sync_loop(sync_rx, dir, durable_sequence, poison)
        });

        let writer = tokio::task::spawn_blocking({
            let state = WriterState {
                options: options.clone(),
                next_sequence,
                next_segment_number: recovery.last_segment_number.saturating_add(1).max(1),
                last_written_sequence: recovery.last_sequence,
                active: None,
                dirty_files: Vec::new(),
                directory_dirty: false,
                earliest_sequence: earliest_sequence.clone(),
                poison: poison.clone(),
            };
            move || writer_loop(writer_rx, sync_tx, state)
        });

        let batcher = tokio::spawn(batcher_loop(
            ingress_rx,
            writer_tx,
            options.batch_max_records,
            options.batch_max_bytes,
            options.batch_linger,
        ));

        Ok(Arc::new(Self {
            options,
            ingress_tx,
            durable_sequence,
            earliest_sequence,
            poison,
            closed: AtomicBool::new(false),
            lifecycle: tokio::sync::RwLock::new(()),
            shutdown_lock: tokio::sync::Mutex::new(()),
            handles: tokio::sync::Mutex::new(Some(WorkerHandles {
                batcher,
                writer,
                syncer,
            })),
        }))
    }

    fn current_error(&self) -> Option<WalError> {
        self.poison.lock().ok().and_then(|error| error.clone())
    }
}

#[async_trait]
impl Wal for SegmentWal {
    type Reader = SegmentWalReader;

    async fn append(&self, payload: Bytes, sync: bool) -> Result<Sequence, WalError> {
        let lifecycle = self.lifecycle.read().await;
        if payload.len() > self.options.max_record_size {
            return Err(WalError::PayloadTooLarge {
                actual: payload.len(),
                maximum: self.options.max_record_size,
            });
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(WalError::Closed);
        }
        if let Some(error) = self.current_error() {
            return Err(error);
        }

        let (response, receiver) = oneshot::channel();
        self.ingress_tx
            .send(IngressCommand::Append(AppendRequest {
                payload,
                sync,
                response,
            }))
            .await
            .map_err(|_| WalError::Closed)?;
        drop(lifecycle);
        receiver.await.map_err(|_| WalError::Closed)?
    }

    async fn new_reader(&self, from_sequence: Sequence) -> Result<Self::Reader, WalError> {
        if let Some(error) = self.current_error() {
            return Err(error);
        }

        let durable = decode_optional_sequence(self.durable_sequence.load(Ordering::Acquire));
        let earliest = decode_optional_sequence(self.earliest_sequence.load(Ordering::Acquire));
        if let Some(earliest) = earliest
            && from_sequence < earliest
        {
            return Err(WalError::SequenceExpired {
                requested: from_sequence,
                earliest,
            });
        }

        let dir = self.options.dir.clone();
        let files = tokio::task::spawn_blocking(move || list_segment_files(&dir))
            .await
            .map_err(|error| WalError::Worker(error.to_string()))??;
        Ok(SegmentWalReader::new(files, from_sequence, durable))
    }

    async fn shutdown(&self) -> Result<(), WalError> {
        let _shutdown_guard = self.shutdown_lock.lock().await;
        let mut handles_guard = self.handles.lock().await;
        let Some(handles) = handles_guard.take() else {
            return Ok(());
        };

        let lifecycle = self.lifecycle.write().await;
        self.closed.store(true, Ordering::Release);
        let (response, receiver) = oneshot::channel();
        let send_result = self
            .ingress_tx
            .send(IngressCommand::Shutdown(response))
            .await;
        drop(lifecycle);
        let result = if send_result.is_err() {
            Err(WalError::Closed)
        } else {
            receiver.await.unwrap_or(Err(WalError::Closed))
        };

        for handle in [handles.batcher, handles.writer, handles.syncer] {
            handle
                .await
                .map_err(|error| WalError::Worker(error.to_string()))?;
        }
        result
    }
}

async fn batcher_loop(
    mut ingress_rx: mpsc::Receiver<IngressCommand>,
    writer_tx: mpsc::Sender<WriterCommand>,
    max_records: usize,
    max_bytes: usize,
    linger: std::time::Duration,
) {
    let mut pending = None;
    loop {
        let command = match pending.take() {
            Some(command) => command,
            None => match ingress_rx.recv().await {
                Some(command) => command,
                None => {
                    send_internal_shutdown(&writer_tx).await;
                    return;
                }
            },
        };
        let IngressCommand::Append(first) = command else {
            if let IngressCommand::Shutdown(response) = command {
                let _ = writer_tx.send(WriterCommand::Shutdown(response)).await;
            }
            return;
        };

        let mut bytes = first.payload.len();
        let mut batch = vec![first];
        let deadline = tokio::time::Instant::now() + linger;
        let mut shutdown = None;
        let mut ingress_closed = false;

        while batch.len() < max_records && bytes < max_bytes {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                command = ingress_rx.recv() => {
                    match command {
                        Some(IngressCommand::Append(request)) => {
                            if !batch.is_empty() && bytes + request.payload.len() > max_bytes {
                                pending = Some(IngressCommand::Append(request));
                                break;
                            }
                            bytes += request.payload.len();
                            batch.push(request);
                        }
                        Some(IngressCommand::Shutdown(response)) => {
                            shutdown = Some(response);
                            break;
                        }
                        None => {
                            ingress_closed = true;
                            break;
                        },
                    }
                }
            }
        }

        if let Err(error) = writer_tx.send(WriterCommand::Batch(batch)).await {
            fail_writer_command(error.0, WalError::Worker("WAL writer stopped".into()));
            if let Some(response) = shutdown {
                let _ = response.send(Err(WalError::Worker("WAL writer stopped".into())));
            }
            return;
        }

        if let Some(response) = shutdown {
            let _ = writer_tx.send(WriterCommand::Shutdown(response)).await;
            return;
        }
        if ingress_closed {
            send_internal_shutdown(&writer_tx).await;
            return;
        }
    }
}

async fn send_internal_shutdown(writer_tx: &mpsc::Sender<WriterCommand>) {
    let (response, _receiver) = oneshot::channel();
    let _ = writer_tx.send(WriterCommand::Shutdown(response)).await;
}

fn writer_loop(
    mut writer_rx: mpsc::Receiver<WriterCommand>,
    sync_tx: mpsc::Sender<SyncCommand>,
    mut state: WriterState,
) {
    while let Some(command) = writer_rx.blocking_recv() {
        match command {
            WriterCommand::Batch(batch) => state.write_batch(batch, &sync_tx),
            WriterCommand::Shutdown(response) => {
                state.send_shutdown(response, &sync_tx);
                return;
            }
        }
    }
}

impl WriterState {
    fn write_batch(&mut self, batch: Vec<AppendRequest>, sync_tx: &mpsc::Sender<SyncCommand>) {
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

        let first_sequence = records.first().unwrap().0;
        let last_sequence = records.last().unwrap().0;
        self.next_sequence = next;
        self.last_written_sequence = Some(last_sequence);
        let _ = self.earliest_sequence.compare_exchange(
            NO_SEQUENCE,
            first_sequence,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

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
        if let Err(error) = sync_tx.blocking_send(SyncCommand::Sync(point)) {
            let wal_error = WalError::Worker("WAL sync worker stopped".into());
            if let SyncCommand::Sync(point) = error.0 {
                fail_waiters(point.waiters, wal_error.clone());
            }
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
                && active.offset + encoded.len() as u64 > self.options.max_segment_size
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
            through_sequence: self.last_written_sequence,
            waiters,
        }
    }

    fn send_shutdown(
        &mut self,
        response: oneshot::Sender<Result<(), WalError>>,
        sync_tx: &mpsc::Sender<SyncCommand>,
    ) {
        let point = self.take_sync_point(Vec::new());
        if let Err(error) = sync_tx.blocking_send(SyncCommand::Shutdown { point, response })
            && let SyncCommand::Shutdown { response, .. } = error.0
        {
            let _ = response.send(Err(WalError::Worker("WAL sync worker stopped".into())));
        }
    }
}

fn sync_loop(
    mut sync_rx: mpsc::Receiver<SyncCommand>,
    dir: PathBuf,
    durable_sequence: Arc<AtomicU64>,
    poison: Arc<Mutex<Option<WalError>>>,
) {
    while let Some(command) = sync_rx.blocking_recv() {
        let mut point;
        let mut shutdown_response = None;
        match command {
            SyncCommand::Sync(sync_point) => point = sync_point,
            SyncCommand::Shutdown {
                point: sync_point,
                response,
            } => {
                point = sync_point;
                shutdown_response = Some(response);
            }
        }

        while shutdown_response.is_none() {
            let Ok(command) = sync_rx.try_recv() else {
                break;
            };
            match command {
                SyncCommand::Sync(next) => merge_sync_points(&mut point, next),
                SyncCommand::Shutdown {
                    point: next,
                    response,
                } => {
                    merge_sync_points(&mut point, next);
                    shutdown_response = Some(response);
                }
            }
        }

        let result = if let Some(error) = locked_error(&poison) {
            Err(error)
        } else {
            perform_sync(&point, &dir).map_err(WalError::from)
        };

        match result {
            Ok(()) => {
                if let Some(sequence) = point.through_sequence {
                    durable_sequence.store(sequence, Ordering::Release);
                }
                for (sequence, waiter) in point.waiters {
                    let _ = waiter.send(Ok(sequence));
                }
                if let Some(response) = shutdown_response {
                    let _ = response.send(Ok(()));
                    return;
                }
            }
            Err(error) => {
                set_poison(&poison, error.clone());
                fail_waiters(point.waiters, error.clone());
                if let Some(response) = shutdown_response {
                    let _ = response.send(Err(error));
                    return;
                }
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
    target.through_sequence = match (target.through_sequence, next.through_sequence) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    };
    target.waiters.append(&mut next.waiters);
}

fn fail_writer_command(command: WriterCommand, error: WalError) {
    match command {
        WriterCommand::Batch(batch) => fail_batch(batch, error),
        WriterCommand::Shutdown(response) => {
            let _ = response.send(Err(error));
        }
    }
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

fn decode_optional_sequence(sequence: u64) -> Option<u64> {
    (sequence != NO_SEQUENCE).then_some(sequence)
}
