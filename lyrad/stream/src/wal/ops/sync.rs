//! Internal operations handled by the WAL sync thread.

use super::super::Sequence;
use super::super::error::WalError;
use super::super::segment::FileHandle;
use std::sync::Arc;
use tokio::sync::oneshot;

pub(in crate::wal) type SyncWaiter = (Sequence, oneshot::Sender<Result<Sequence, WalError>>);

pub(in crate::wal) struct SyncFile {
    pub(in crate::wal) file: Arc<FileHandle>,
    pub(in crate::wal) end: u64,
}

pub(in crate::wal) struct SyncOp {
    pub(in crate::wal) files: Vec<SyncFile>,
    pub(in crate::wal) sync_directory: bool,
    pub(in crate::wal) last_sequence: Sequence,
    pub(in crate::wal) waiters: Vec<SyncWaiter>,
}
