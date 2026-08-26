//! Internal operations handled by the WAL worker threads.

use super::Sequence;
use super::error::WalError;
use super::segment::FileHandle;
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::oneshot;

pub type SyncWaiter = (Sequence, oneshot::Sender<Result<Sequence, WalError>>);

pub struct AppendOp {
    pub payload: Bytes,
    pub response: oneshot::Sender<Result<Sequence, WalError>>,
}

pub struct SyncFile {
    pub file: Arc<FileHandle>,
    pub end: u64,
}

pub struct SyncOp {
    pub files: Vec<SyncFile>,
    pub sync_directory: bool,
    pub last_sequence: Sequence,
    pub waiters: Vec<SyncWaiter>,
}

pub(super) enum Operation {
    Append(AppendOp),
    Sync(SyncOp),
    Close,
}
