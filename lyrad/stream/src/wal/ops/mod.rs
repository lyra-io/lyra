//! Internal operations handled by the WAL worker threads.

use super::Sequence;
use super::error::WalError;
use super::segment::FileSegment;
use bytes::Bytes;
use meta::utils::promise::PromiseHandle;

pub struct AppendOp {
    pub payload: Bytes,
    pub handle: PromiseHandle<Sequence, WalError>,
}

pub struct SyncOp {
    pub segments: Vec<FileSegment>,
    pub sync_directory: bool,
    pub completion: Option<(Sequence, PromiseHandle<Sequence, WalError>)>,
}

pub(super) enum Operation {
    Append(AppendOp),
    Sync(SyncOp),
    Close,
}
