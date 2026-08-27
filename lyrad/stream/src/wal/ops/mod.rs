//! Internal operations handled by the WAL worker threads.

use super::Sequence;
use super::error::WalError;
use super::segment::SegmentSyncHandle;
use bytes::Bytes;
use meta::utils::promise::PromiseHandle;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

pub(super) type AppendHandle = PromiseHandle<Sequence, WalError>;
pub(super) type AdvancedSequence = (Option<Sequence>, Option<SegmentSyncHandle>);
pub(super) type AppendCompletion = (Sequence, AppendHandle);
pub(super) type DirtySegmentQueue = Arc<Mutex<VecDeque<SegmentSyncHandle>>>;

pub struct AppendOp {
    pub payload: Bytes,
    pub handle: AppendHandle,
}

pub(super) enum Operation {
    Append(AppendOp),
    Close,
}
