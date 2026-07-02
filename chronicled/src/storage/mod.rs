
mod ss;
mod wal;
mod write_cache;
mod storage;
mod buffer;
mod cache;

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
