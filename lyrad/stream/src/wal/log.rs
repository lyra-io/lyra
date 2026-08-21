//! Stateful write-ahead log implementation.

use super::append_command::{AppendCommand, AppendRequest};
use super::error::WalError;
use super::options::LogOptions;
use super::{Lifecycle, MAX_INFLIGHT_APPEND_NUM, Sequence, WAL_SEGMENT_SIZE, Wal};
use crate::segment::{
    AlignedBuffer, FILE_HEADER_SIZE, SegmentFile, SegmentRecord, list_segment_files, scan_segment,
    sync_directory,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// A batching write-ahead log backed by `lyra`'s segment format.
///
/// Appends are queued into an in-memory channel and drained by a single
/// dedicated thread that batches them with `blocking_recv_many`, writes them
/// to aligned segment files, and flushes them to stable storage whenever a
/// sync append requires it or the log shuts down. I/O failures are retried
/// until shutdown cancels the retry loop.
pub struct Log {
    // Control state
    context: CancellationToken,
    append_thread: Mutex<Option<AppendThread>>,

    // Immutable state
    inflight_tx: mpsc::Sender<AppendCommand>,
    options: LogOptions,

    // Mutable state
    state: Arc<RwLock<Lifecycle>>,
}

const RETRY_DELAY: Duration = Duration::from_millis(10);

struct AppendThread {
    // Control state
    handle: JoinHandle<()>,
    done: oneshot::Receiver<()>,
}

impl Log {
    /// Opens the WAL at `options.dir`.
    ///
    /// Existing segment files from a previous run are scanned and recovered:
    /// sequence numbering resumes after the last durable record, and new
    /// appends continue in a fresh segment.
    pub async fn open(options: LogOptions) -> Result<Arc<Self>, WalError> {
        tokio::fs::create_dir_all(&options.dir).await?;
        let (next_sequence, next_segment_number) = recover_state(&options.dir)?;

        let state = Arc::new(RwLock::new(Lifecycle::default()));
        let context = CancellationToken::new();
        let (inflight_tx, inflight_rx) = mpsc::channel(MAX_INFLIGHT_APPEND_NUM);
        let (done_tx, done) = oneshot::channel();

        let handle = std::thread::Builder::new()
            .name("lyra-wal-append".into())
            .spawn({
                let context = context.clone();
                let options = options.clone();
                move || {
                    append_loop(
                        context,
                        inflight_rx,
                        options,
                        next_sequence,
                        next_segment_number,
                    );
                    let _ = done_tx.send(());
                }
            })?;

        Ok(Arc::new(Self {
            context,
            append_thread: Mutex::new(Some(AppendThread { handle, done })),
            inflight_tx,
            options,
            state,
        }))
    }
}

#[async_trait]
impl Wal for Log {
    async fn append(&self, payload: Bytes, sync: bool) -> Result<Sequence, WalError> {
        let permit = self
            .inflight_tx
            .reserve()
            .await
            .map_err(|_| WalError::Closed)?;
        let state = self.state.read().await;
        if *state != Lifecycle::Running {
            return Err(WalError::Closed);
        }

        let (response, receiver) = oneshot::channel();
        permit.send(AppendCommand::Append(AppendRequest {
            payload,
            sync,
            response,
        }));
        drop(state);
        receiver.await.map_err(|_| WalError::Closed)?
    }

    async fn read(&self, sequence: Sequence) -> Result<Option<Bytes>, WalError> {
        let segment_files = list_segment_files(&self.options.dir)?;
        let last_index = segment_files.len().saturating_sub(1);
        for (index, (file_number, path)) in segment_files.into_iter().enumerate() {
            let scan = scan_segment(&path, index == last_index)?;
            validate_segment_number(file_number, scan.segment_number, &path)?;
            for record in scan.records {
                let record_sequence = decode_sequence(&path, &record)?;
                if record_sequence == sequence {
                    return Ok(Some(Bytes::copy_from_slice(&record[8..])));
                }
                if record_sequence > sequence {
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }

    async fn shutdown(&self) -> Result<(), WalError> {
        let mut thread_guard = self.append_thread.lock().await;
        let Some(append_thread) = thread_guard.take() else {
            return Ok(());
        };
        drop(thread_guard);

        {
            let mut state = self.state.write().await;
            *state = Lifecycle::Draining;
            self.context.cancel();
        }

        let _ = self.inflight_tx.send(AppendCommand::Shutdown).await;
        let _ = append_thread.done.await;
        let join_error = append_thread
            .handle
            .join()
            .err()
            .map(|panic| WalError::Worker(panic_message(panic)));
        {
            let mut state = self.state.write().await;
            *state = Lifecycle::Closed;
        }
        join_error.map_or(Ok(()), Err)
    }
}

fn append_loop(
    context: CancellationToken,
    mut inflight_rx: mpsc::Receiver<AppendCommand>,
    options: LogOptions,
    mut next_sequence: Sequence,
    mut next_segment_number: u64,
) {
    let max_batch = MAX_INFLIGHT_APPEND_NUM;
    // Active segment bookkeeping: (segment number, file handle, write offset).
    let mut active: Option<(u64, Arc<SegmentFile>, u64)> = None;
    let mut dirty_files: Vec<Arc<SegmentFile>> = Vec::new();
    let mut directory_dirty = false;

    loop {
        let mut events = Vec::new();
        let received = inflight_rx.blocking_recv_many(&mut events, max_batch);
        if received == 0 {
            break;
        }

        let mut batch = Vec::new();
        let mut stopping = false;
        for event in events {
            match event {
                AppendCommand::Append(request) => batch.push(request),
                AppendCommand::Shutdown => {
                    inflight_rx.close();
                    stopping = true;
                }
            }
        }
        if batch.is_empty() {
            if stopping {
                break;
            }
            continue;
        }

        let records = assign_records(&batch, &mut next_sequence);
        if !retry_until(&context, || {
            write_records(
                &records,
                &mut next_segment_number,
                &mut active,
                &mut dirty_files,
                &mut directory_dirty,
                &options,
            )
        }) {
            if stopping {
                break;
            }
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
        if !synced_waiters.is_empty()
            && retry_until(&context, || {
                perform_sync(&dirty_files, directory_dirty, &options.dir).map_err(WalError::from)
            })
        {
            dirty_files.clear();
            directory_dirty = false;
            for (sequence, waiter) in synced_waiters {
                let _ = waiter.send(Ok(sequence));
            }
        }

        if stopping {
            break;
        }
    }

    // Final flush of anything still dirty; all callers have already been
    // answered or dropped, so a failure here is only a lost final flush.
    let _ = perform_sync(&dirty_files, directory_dirty, &options.dir);
}

/// Scans the existing segments in `dir` and returns the sequence to assign to
/// the next append and the number of the next segment file to create.
fn recover_state(dir: &Path) -> Result<(Sequence, u64), WalError> {
    let mut next_sequence: Sequence = 0;
    let mut max_segment_number: u64 = 0;
    let segment_files = list_segment_files(dir)?;
    let last_index = segment_files.len().saturating_sub(1);
    for (index, (file_number, path)) in segment_files.into_iter().enumerate() {
        let scan = scan_segment(&path, index == last_index)?;
        validate_segment_number(file_number, scan.segment_number, &path)?;
        if index == last_index {
            truncate_torn_tail(&path, scan.valid_len)?;
        }
        max_segment_number = max_segment_number.max(file_number);
        for record in scan.records {
            let sequence = decode_sequence(&path, &record)?;
            if sequence != next_sequence {
                return Err(WalError::Corruption {
                    path,
                    message: format!("expected WAL sequence {next_sequence}, found {sequence}"),
                });
            }
            next_sequence = next_sequence
                .checked_add(1)
                .ok_or_else(|| WalError::Worker("WAL sequence space exhausted".into()))?;
        }
    }
    let next_segment_number = max_segment_number
        .checked_add(1)
        .ok_or_else(|| WalError::Worker("WAL segment number space exhausted".into()))?;
    Ok((next_sequence, next_segment_number))
}

fn truncate_torn_tail(path: &Path, valid_len: u64) -> Result<(), WalError> {
    if std::fs::metadata(path)?.len() == valid_len {
        return Ok(());
    }
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    file.set_len(valid_len)?;
    file.sync_data()?;
    Ok(())
}

fn validate_segment_number(
    file_number: u64,
    header_number: u64,
    path: &Path,
) -> Result<(), WalError> {
    if file_number == header_number {
        return Ok(());
    }
    Err(WalError::Corruption {
        path: path.to_path_buf(),
        message: format!(
            "segment filename identifies {file_number}, but its header identifies {header_number}"
        ),
    })
}

fn decode_sequence(path: &Path, record: &[u8]) -> Result<Sequence, WalError> {
    let prefix = record.get(..8).ok_or_else(|| WalError::Corruption {
        path: path.to_path_buf(),
        message: "record shorter than the sequence prefix".into(),
    })?;
    Ok(u64::from_le_bytes(prefix.try_into().unwrap()))
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
    context: &CancellationToken,
    mut attempt: impl FnMut() -> Result<(), E>,
) -> bool {
    loop {
        match attempt() {
            Ok(()) => return true,
            Err(_) if context.is_cancelled() => return false,
            Err(error) => {
                tracing::warn!(error = %error, "WAL operation failed; it will be retried");
                std::thread::sleep(RETRY_DELAY);
            }
        }
    }
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "WAL append thread panicked".into()
    }
}

fn write_records(
    records: &[(Sequence, Bytes)],
    next_segment_number: &mut u64,
    active: &mut Option<(u64, Arc<SegmentFile>, u64)>,
    dirty_files: &mut Vec<Arc<SegmentFile>>,
    directory_dirty: &mut bool,
    options: &LogOptions,
) -> Result<(), WalError> {
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
) -> Result<(), WalError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn standard_options(path: &Path) -> LogOptions {
        let mut options = LogOptions::new(path);
        options.io_mode = crate::segment::IoMode::Standard;
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
    async fn rotates_and_reads_records_across_segments() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        let payloads: Vec<_> = (0..4u8)
            .map(|value| Bytes::from(vec![value; 128]))
            .collect();
        seed_segments(&options, &payloads, 8192);

        assert_eq!(list_segment_files(dir.path()).unwrap().len(), 4);
        let log = Log::open(options).await.unwrap();
        for (sequence, payload) in payloads.iter().enumerate() {
            assert_eq!(
                log.read(sequence as Sequence).await.unwrap().unwrap(),
                *payload
            );
        }
        assert_eq!(log.read(100).await.unwrap(), None);
        assert_eq!(
            log.append(Bytes::from_static(b"next"), true).await.unwrap(),
            4
        );
        log.shutdown().await.unwrap();
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
        std::fs::OpenOptions::new()
            .write(true)
            .open(&files[0].1)
            .unwrap()
            .set_len(FILE_HEADER_SIZE as u64 + 4)
            .unwrap();

        let error = Log::open(options).await.err().unwrap();
        assert!(matches!(error, WalError::Corruption { .. }));
    }

    #[tokio::test]
    async fn recovery_truncates_a_torn_final_segment_before_continuing() {
        let dir = tempfile::tempdir().unwrap();
        let options = standard_options(dir.path());
        let payloads = vec![Bytes::from(vec![0x11; 128]), Bytes::from(vec![0x22; 128])];
        seed_segments(&options, &payloads, WAL_SEGMENT_SIZE);

        let path = list_segment_files(dir.path()).unwrap()[0].1.clone();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len((FILE_HEADER_SIZE * 2 + 4) as u64)
            .unwrap();

        let log = Log::open(options.clone()).await.unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 8192);
        assert_eq!(log.read(0).await.unwrap().unwrap(), payloads[0]);
        assert_eq!(log.read(1).await.unwrap(), None);
        assert_eq!(
            log.append(Bytes::from_static(b"replacement"), true)
                .await
                .unwrap(),
            1
        );
        log.shutdown().await.unwrap();

        let reopened = Log::open(options).await.unwrap();
        assert_eq!(
            reopened.read(1).await.unwrap().unwrap(),
            Bytes::from_static(b"replacement")
        );
        reopened.shutdown().await.unwrap();
    }
}
