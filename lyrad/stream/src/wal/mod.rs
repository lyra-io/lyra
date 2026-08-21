//! Stateful write-ahead log API.

mod append_command;
mod error;
mod log;
mod options;

pub use crate::segment::IoMode;
pub use error::WalError;
pub use log::Log;
pub use options::LogOptions;

use async_trait::async_trait;
use bytes::Bytes;

/// A monotonically increasing WAL record identifier.
pub type Sequence = u64;

/// Target size at which the WAL rotates to a new segment file.
///
/// A single record may exceed this size.
pub(crate) const WAL_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// Maximum number of appends buffered while the writer is busy.
pub(crate) const MAX_INFLIGHT_APPEND_NUM: usize = 4096;

/// Lifecycle of the log, consulted by appends before enqueuing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Lifecycle {
    /// The log is open and accepts appends.
    #[default]
    Running,
    /// Shutdown has begun; new appends are rejected while pending ones drain.
    Draining,
    /// All background workers have stopped.
    Closed,
}

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

    /// Reads the payload durably stored at `sequence`, if any.
    ///
    /// Returns `None` when the sequence has not been written, or when its
    /// only record was part of a torn tail lost in an unclean shutdown.
    async fn read(&self, sequence: Sequence) -> Result<Option<Bytes>, WalError>;

    /// Drains pending appends, flushes them to stable storage, and stops all
    /// background workers. Idempotent; after shutdown, [`Wal::append`] fails
    /// with [`WalError::Closed`].
    async fn shutdown(&self) -> Result<(), WalError>;
}
