mod error;
mod options;
mod runtime;

pub use crate::segment::IoMode;
pub use error::WalError;
pub use options::WalOptions;
pub use runtime::SegmentWal;

use async_trait::async_trait;
use bytes::Bytes;

/// A monotonically increasing WAL record identifier.
pub type Sequence = u64;

/// A write-ahead log that assigns an ordered [`Sequence`] to every appended
/// payload.
#[async_trait]
pub trait Wal: Send + Sync + 'static {
    /// Appends `payload` and returns the sequence assigned to it.
    ///
    /// When `sync` is true, the returned future completes only after the
    /// record and any preceding records have been flushed to stable storage.
    /// When `sync` is false, it completes as soon as the record is written to
    /// the operating system; all records still become durable on
    /// [`Wal::shutdown`].
    async fn append(&self, payload: Bytes, sync: bool) -> Result<Sequence, WalError>;

    /// Drains pending appends, flushes them to stable storage, and stops all
    /// background workers. Idempotent; after shutdown, [`Wal::append`] fails
    /// with [`WalError::Closed`].
    async fn shutdown(&self) -> Result<(), WalError>;
}
