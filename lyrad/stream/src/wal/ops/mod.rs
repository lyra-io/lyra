//! Internal operations handled by the WAL worker threads.

use super::Sequence;
use super::error::WalError;
use super::segment::FileSegment;
use bytes::Bytes;
use tokio::sync::oneshot;

pub struct AppendOp {
    pub payload: Bytes,
    pub result_tx: oneshot::Sender<Result<Sequence, WalError>>,
}

pub struct SyncOp {
    pub segments: Vec<FileSegment>,
    pub sync_directory: bool,
    pub completion: Option<(Sequence, oneshot::Sender<Result<Sequence, WalError>>)>,
}

pub(super) enum Operation {
    Append(AppendOp),
    Sync(SyncOp),
    Close,
}
