//! Stateful write-ahead log API.

mod error;
mod log;
mod ops;
mod options;
mod publisher;
mod segment;

pub use error::WalError;
pub use log::SegmentLog;
pub use options::LogOptions;
pub use publisher::{PublishBatch, PublishTarget};

use async_trait::async_trait;
use bytes::Bytes;

/// A monotonically increasing WAL record identifier.
pub type Sequence = u64;

/// Maximum encoded WAL segment size before rotation.
pub(crate) const WAL_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Maximum number of appends buffered while the writer is busy.
pub(crate) const MAX_INFLIGHT_APPEND_NUM: usize = 4096;

/// Maximum number of written batches waiting behind the active publisher.
pub(crate) const MAX_PENDING_PUBLISH_BATCH_NUM: usize = 1;

/// An ordered, durable stream log.
#[async_trait]
pub trait Log: Send + Sync {
    /// Appends `payload` and returns its assigned sequence.
    ///
    /// The append is acknowledged after its sequence becomes durable.
    async fn append(&self, payload: Bytes) -> Result<Sequence, WalError>;

    /// Stops admission, drains owned work, and closes all components.
    async fn close(&self);
}

/// Whether the log still accepts appends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lifecycle {
    /// The log is open and accepts appends.
    Running,
    /// Close has begun and new appends are rejected.
    Closing,
    /// All owned workers and components have been closed.
    Closed,
}
