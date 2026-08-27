//! Internal operations handled by the WAL worker threads.

use super::Sequence;
use super::error::WalError;
use bytes::Bytes;
use meta::utils::promise::PromiseHandle;

pub(super) type AppendHandle = PromiseHandle<Sequence, WalError>;

pub struct AppendOp {
    pub payload: Bytes,
    pub handle: AppendHandle,
}

pub(super) enum Operation {
    Append(AppendOp),
    Close,
}
