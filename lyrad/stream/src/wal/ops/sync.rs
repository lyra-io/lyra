//! Internal operations handled by the WAL sync thread.

use super::super::Sequence;
use super::super::error::LogError;
use crate::segment::VfsFile;
use std::sync::Arc;
use tokio::sync::oneshot;

pub(in crate::wal) type SyncWaiter = (Sequence, oneshot::Sender<Result<Sequence, LogError>>);

pub(in crate::wal) struct SyncOp {
    pub(in crate::wal) files: Vec<Arc<VfsFile>>,
    pub(in crate::wal) sync_directory: bool,
    pub(in crate::wal) last_sequence: Sequence,
    pub(in crate::wal) waiters: Vec<SyncWaiter>,
}
