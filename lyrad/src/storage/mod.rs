mod buffer;
mod cache;
mod ss;
mod storage;
pub mod wal;

use crate::error::unit_error::UnitError;
use lyra_proto::pb_ext::Event;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use wal::WalOptions;

#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    async fn append(&self, data: Vec<u8>) -> Result<i64, UnitError>;

    fn watch_synced(&self) -> watch::Receiver<i64>;

    async fn apply_write(&self, event: Event, truncate: bool);

    async fn read_events(
        &self,
        timeline_id: i64,
        start_offset: i64,
        end_offset: i64,
    ) -> Result<Vec<Event>, UnitError>;

    fn check_term(&self, timeline_id: i64, request_term: i64) -> Result<(), i64>;

    fn fence(&self, timeline_id: i64, new_term: i64) -> Result<i64, i64>;

    fn update_lra(&self, timeline_id: i64, lra: i64);

    async fn shutdown(&self);
}

pub struct UnitStorage {
    next_wal_offset: AtomicI64,
    synced_tx: watch::Sender<i64>,
    state: Arc<Mutex<UnitStorageState>>,
}

#[derive(Default)]
struct UnitStorageState {
    terms: HashMap<i64, i64>,
    lra: HashMap<i64, i64>,
    events: HashMap<i64, Vec<Event>>,
}

impl UnitStorage {
    pub async fn open(options: WalOptions) -> Result<Self, UnitError> {
        tokio::fs::create_dir_all(&options.dir)
            .await
            .map_err(|error| UnitError::Storage(error.to_string()))?;
        let _ = (options.max_segment_size, options.io_mode);
        let (synced_tx, _) = watch::channel(-1);
        Ok(Self {
            next_wal_offset: AtomicI64::new(0),
            synced_tx,
            state: Arc::new(Mutex::new(UnitStorageState::default())),
        })
    }
}

#[async_trait::async_trait]
impl Storage for UnitStorage {
    async fn append(&self, _data: Vec<u8>) -> Result<i64, UnitError> {
        let offset = self.next_wal_offset.fetch_add(1, Ordering::SeqCst);
        let _ = self.synced_tx.send(offset);
        Ok(offset)
    }

    fn watch_synced(&self) -> watch::Receiver<i64> {
        self.synced_tx.subscribe()
    }

    async fn apply_write(&self, event: Event, truncate: bool) {
        let mut state = self.state.lock().unwrap();
        let events = state.events.entry(event.timeline_id).or_default();
        if truncate {
            events.retain(|stored| stored.offset < event.offset);
        }
        events.push(event);
        events.sort_by_key(|stored| stored.offset);
    }

    async fn read_events(
        &self,
        timeline_id: i64,
        start_offset: i64,
        end_offset: i64,
    ) -> Result<Vec<Event>, UnitError> {
        let state = self
            .state
            .lock()
            .map_err(|_| UnitError::Storage("unit storage lock poisoned".into()))?;
        Ok(state
            .events
            .get(&timeline_id)
            .map(|events| {
                events
                    .iter()
                    .filter(|event| event.offset >= start_offset && event.offset < end_offset)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    fn check_term(&self, timeline_id: i64, request_term: i64) -> Result<(), i64> {
        let state = self.state.lock().unwrap();
        let current = state.terms.get(&timeline_id).copied().unwrap_or_default();
        if request_term < current {
            Err(current)
        } else {
            Ok(())
        }
    }

    fn fence(&self, timeline_id: i64, new_term: i64) -> Result<i64, i64> {
        let mut state = self.state.lock().unwrap();
        let current = state.terms.get(&timeline_id).copied().unwrap_or_default();
        if new_term < current {
            return Err(current);
        }
        state.terms.insert(timeline_id, new_term);
        Ok(state.lra.get(&timeline_id).copied().unwrap_or_default())
    }

    fn update_lra(&self, timeline_id: i64, lra: i64) {
        let mut state = self.state.lock().unwrap();
        state
            .lra
            .entry(timeline_id)
            .and_modify(|current| *current = (*current).max(lra))
            .or_insert(lra);
    }

    async fn shutdown(&self) {}
}
