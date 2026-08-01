mod error;
mod format;
mod io;
mod options;
mod reader;
mod runtime;

pub use error::WalError;
pub use options::{IoMode, WalOptions};
pub use reader::SegmentWalReader;
pub use runtime::SegmentWal;

use async_trait::async_trait;
use bytes::Bytes;

pub type Sequence = u64;

#[async_trait]
pub trait Wal: Send + Sync + 'static {
    type Reader: WalReader;

    async fn append(&self, payload: Bytes, sync: bool) -> Result<Sequence, WalError>;

    async fn new_reader(&self, from_sequence: Sequence) -> Result<Self::Reader, WalError>;

    async fn shutdown(&self) -> Result<(), WalError>;
}

#[async_trait]
pub trait WalReader: Send {
    async fn read_next(&mut self) -> Result<Option<(Sequence, Bytes)>, WalError>;
}
