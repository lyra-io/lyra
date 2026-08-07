mod error;
mod format;
mod options;
mod recovery;
mod retention;
mod runtime;

pub use crate::segment::IoMode;
pub use error::WalError;
pub use options::WalOptions;
pub use recovery::WalRecovery;
pub use runtime::SegmentWal;

use async_trait::async_trait;
use bytes::Bytes;

/// A monotonically increasing WAL record identifier.
pub type Sequence = u64;

/// A write-ahead log that assigns an ordered [`Sequence`] to every appended
/// payload.
///
/// Records are appended in sequence order and can be replayed with
/// [`Wal::recover`]. A recovery is a durable snapshot: it only yields records
/// whose durability was confirmed as of the call, and records appended later
/// (even to the same sequence space) do not appear in an already-created
/// iterator.
#[async_trait]
pub trait Wal: Send + Sync + 'static {
    /// Iterator over recovered `(sequence, payload)` records.
    type Recovery: Iterator<Item = Result<(Sequence, Bytes), WalError>> + Send;

    /// Appends `payload` and returns the sequence assigned to it.
    ///
    /// When `sync` is true, the returned future completes only after the
    /// record and any preceding records have been flushed to stable storage.
    /// When `sync` is false, it completes as soon as the record is written to
    /// the operating system; all records still become durable on
    /// [`Wal::shutdown`].
    async fn append(&self, payload: Bytes, sync: bool) -> Result<Sequence, WalError>;

    /// Replays records with sequence greater than or equal to `from_sequence`.
    ///
    /// Fails with [`WalError::SequenceExpired`] if `from_sequence` has already
    /// been trimmed. The returned iterator only contains records durable at
    /// the time of this call.
    fn recover(&self, from_sequence: Sequence) -> Result<Self::Recovery, WalError>;

    /// Drains pending appends, flushes them to stable storage, and stops all
    /// background workers. Idempotent; after shutdown, [`Wal::append`] fails
    /// with [`WalError::Closed`].
    async fn shutdown(&self) -> Result<(), WalError>;
}
