//! Internal operations handled by the WAL writer thread.

use super::super::Sequence;
use super::super::error::LogError;
use bytes::Bytes;
use tokio::sync::oneshot;

pub(in crate::wal) struct AppendOp {
    pub(in crate::wal) payload: Bytes,
    pub(in crate::wal) sync: bool,
    pub(in crate::wal) response: oneshot::Sender<Result<Sequence, LogError>>,
}
