//! Internal operations handled by the WAL worker threads.

use super::Sequence;
use super::error::WalError;
use super::segment::FileSegment;
use bytes::Bytes;
use tokio::sync::oneshot;

pub struct AppendOp {
    pub payload: Bytes,
    pub sequence_tx: oneshot::Sender<Result<Sequence, WalError>>,
}

pub struct SyncOp {
    pub segments: Vec<FileSegment>,
    pub sync_directory: bool,
    pub last_sequence: Sequence,
}

pub(super) enum Operation {
    Append(AppendOp),
    Sync(SyncOp),
    Close,
}
