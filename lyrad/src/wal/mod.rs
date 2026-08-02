mod error;
mod format;
mod options;
mod recovery;
mod runtime;

pub use crate::segment::IoMode;
pub use error::WalError;
pub use options::WalOptions;
pub use recovery::WalRecovery;
pub use runtime::SegmentWal;

use async_trait::async_trait;
use bytes::Bytes;

pub type Sequence = u64;

#[async_trait]
pub trait Wal: Send + Sync + 'static {
    type Recovery: Iterator<Item = Result<(Sequence, Bytes), WalError>> + Send;

    async fn append(&self, payload: Bytes, sync: bool) -> Result<Sequence, WalError>;

    fn recover(&self, from_sequence: Sequence) -> Result<Self::Recovery, WalError>;

    async fn shutdown(&self) -> Result<(), WalError>;
}
