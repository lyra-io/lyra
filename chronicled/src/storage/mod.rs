use crate::error::unit_error::UnitError;
use async_trait::async_trait;
use chronicle_proto::pb_ext::Event;
use tokio::sync::watch;

pub mod segment;
pub mod timeline_state;
pub mod unit_storage;
pub mod wal;
pub mod write_cache;

pub use unit_storage::UnitStorage;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn append(&self, data: Vec<u8>) -> Result<i64, UnitError>;

    fn watch_synced(&self) -> watch::Receiver<i64>;

    async fn apply_write(&self, event: Event, truncate: bool);

    fn check_term(&self, timeline_id: i64, request_term: i64) -> Result<(), i64>;

    fn fence(&self, timeline_id: i64, new_term: i64) -> Result<i64, i64>;

    fn update_lra(&self, timeline_id: i64, lra: i64);

    async fn shutdown(&self);
}
