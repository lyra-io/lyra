use crate::error::unit_error::UnitError;
use crate::storage::Storage;
use crate::storage::timeline_state::TimelineStateManager;
use crate::storage::wal::{Wal, WalOptions};
use crate::storage::write_cache::WriteCache;
use async_trait::async_trait;
use chronicle_proto::pb_ext::Event;
use futures_util::StreamExt;
use prost::Message;
use tokio::sync::watch;
use tracing::{info, warn};

pub struct UnitStorage {
    wal: Wal,
    write_cache: WriteCache,
    timeline_state: TimelineStateManager,
}

impl UnitStorage {
    pub async fn open(options: WalOptions) -> Result<Self, UnitError> {
        let wal = Wal::new(options).await?;
        let storage = Self {
            wal,
            write_cache: WriteCache::new(),
            timeline_state: TimelineStateManager::new(),
        };
        storage.replay_wal().await;
        Ok(storage)
    }

    async fn replay_wal(&self) {
        info!("replaying storage into write cache");
        let mut stream = self.wal.read_stream();
        let mut replayed = 0u64;
        while let Some(result) = stream.next().await {
            match result {
                Ok(data) => {
                    if let Ok(event) = Event::decode(data.as_slice()) {
                        self.write_cache.put_direct(event, false);
                        replayed += 1;
                    }
                }
                Err(e) => {
                    warn!(error = ?e, "storage replay error reading record");
                    break;
                }
            }
        }
        info!(events = replayed, "storage replay complete");
    }

    #[cfg(test)]
    pub fn scan_cached(&self, timeline_id: i64, start_offset: i64, end_offset: i64) -> Vec<Event> {
        self.write_cache.scan(timeline_id, start_offset, end_offset)
    }

    #[cfg(test)]
    pub fn wal(&self) -> &Wal {
        &self.wal
    }
}

#[async_trait]
impl Storage for UnitStorage {
    async fn append(&self, data: Vec<u8>) -> Result<i64, UnitError> {
        self.wal.append(data).await
    }

    fn watch_synced(&self) -> watch::Receiver<i64> {
        self.wal.watch_synced()
    }

    async fn apply_write(&self, event: Event, truncate: bool) {
        self.write_cache.put(event, truncate).await;
    }

    fn check_term(&self, timeline_id: i64, request_term: i64) -> Result<(), i64> {
        self.timeline_state.check_term(timeline_id, request_term)
    }

    fn fence(&self, timeline_id: i64, new_term: i64) -> Result<i64, i64> {
        self.timeline_state.fence(timeline_id, new_term)
    }

    fn update_lra(&self, timeline_id: i64, lra: i64) {
        self.timeline_state.update_lra(timeline_id, lra);
    }

    async fn shutdown(&self) {
        self.wal.shutdown().await;
    }
}
