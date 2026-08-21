//! Commands handled by the WAL append worker.

use super::Sequence;
use super::error::WalError;
use bytes::Bytes;
use tokio::sync::oneshot;

pub(super) struct AppendRequest {
    pub(super) payload: Bytes,
    pub(super) sync: bool,
    pub(super) response: oneshot::Sender<Result<Sequence, WalError>>,
}

pub(super) enum AppendCommand {
    Append(AppendRequest),
    Shutdown,
}
