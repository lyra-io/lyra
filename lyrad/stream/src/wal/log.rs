//! Stateful write-ahead log lifecycle and worker orchestration.

use super::error::WalError;
use super::log_syncer::LogSyncer;
use super::log_writer::LogWriter;
use super::ops::{AppendOp, DirtySegmentQueue, Operation};
use super::options::LogOptions;
use super::{Lifecycle, Log, MAX_INFLIGHT_APPEND_NUM, Sequence};
use crate::vfs::{StandardVfs, Vfs, VfsI};
use async_trait::async_trait;
use bytes::Bytes;
use meta::utils::directory_lock::DirectoryLock;
use meta::utils::logging::utils::log_ignore;
use meta::utils::promise::Promise;
use std::any::Any;
use std::io::ErrorKind;
use std::sync::Arc;
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use std::time::Duration;
use tokio::runtime::Handle as RuntimeHandle;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot, watch};

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

struct WorkerThreads {
    writer: WorkerThread,
    sync: WorkerThread,
}

struct WorkerThread {
    // Control state
    handle: Option<JoinHandle<()>>,
    done: oneshot::Receiver<()>,
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
        let (completion_tx, completion_rx) = mpsc::channel(MAX_INFLIGHT_APPEND_NUM);
        let (advanced_tx, advanced_rx) = watch::channel((None, None));
        let dirty_segments = DirtySegmentQueue::default();

        let (sync_done_tx, sync_done) = oneshot::channel();
        let sync_handle = ThreadBuilder::new().name("lyra-wal-sync".into()).spawn({
            let dir = options.dir.clone();
            let vfs = vfs.clone();
            let dirty_segments = Arc::clone(&dirty_segments);
            let wait_for_sync = options.sync;
            let runtime = RuntimeHandle::current();
            move || {
                LogSyncer::new(
                    advanced_rx,
                    completion_rx,
                    dirty_segments,
                    wait_for_sync,
                    vfs,
                    dir,
                    runtime,
                )
                .run();
                tracing::info!("WAL sync thread exited");
                let _ = sync_done_tx.send(());
            }
        })?;

        let (writer_started_tx, writer_started) = oneshot::channel();
        let (writer_done_tx, writer_done) = oneshot::channel();
        let writer_handle = match ThreadBuilder::new().name("lyra-wal-writer".into()).spawn({
            let options = options.clone();
            let vfs = vfs.clone();
            let dirty_segments = Arc::clone(&dirty_segments);
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
                        LogWriter::new(
                            operation_rx,
                            advanced_tx,
                            dirty_segments,
                            completion_tx,
                            vfs,
                            options,
                        )
                        .map(|writer| (directory_lock, writer))
                    }) {
                    Ok((directory_lock, writer)) => {
                        if writer_started_tx.send(Ok(())).is_ok() {
                            writer.run();
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
            // Control state
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

            // Immutable state
            operation_tx,

            // Mutable state
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

fn panic_message(panic: Box<dyn Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "WAL worker thread panicked".into()
    }
}
