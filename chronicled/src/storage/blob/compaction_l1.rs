use std::sync::Arc;
use std::time::Duration;

use chronicle_proto::pb_ext::Event;
use prost::Message;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::manager::SegmentManager;
use crate::error::unit_error::UnitError;
use crate::storage::index::{IndexEntry, Storage};
use crate::storage::write_cache::WriteCache;
use crate::wal::checkpoint::{self, WalCheckpoint};
use crate::wal::wal::Wal;

pub(crate) struct L1FlushTask {
    pub write_cache: WriteCache,
    pub segment_manager: Arc<SegmentManager>,
    pub index: Storage,
    pub flush_notify: Arc<Notify>,
    pub wal: Option<Wal>,
}

impl L1FlushTask {
    pub async fn run(&self, context: CancellationToken, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    if let Err(e) = self.flush().await {
                        warn!(error = ?e, "final L1 flush failed");
                    }
                    info!("L1 flush task stopped");
                    break;
                }
                _ = ticker.tick() => {
                    if let Err(e) = self.flush().await {
                        warn!(error = ?e, "L1 flush failed");
                    }
                }
                _ = self.flush_notify.notified() => {
                    if let Err(e) = self.flush().await {
                        warn!(error = ?e, "L1 flush failed");
                    }
                }
            }
        }
    }

    pub async fn flush(&self) -> Result<(), UnitError> {
        let sealed = match self.write_cache.sealed_data() {
            Some(s) => s,
            None => return Ok(()),
        };

        if sealed.index.is_empty() {
            self.write_cache.clear_sealed();
            return Ok(());
        }

        let entries_count = sealed.index.len();

        let mut writer = self.segment_manager.new_writer_at_level(1).await?;
        let segment_id = writer.segment_id();
        let mut index_entries = Vec::with_capacity(entries_count);

        for entry in sealed.index.iter() {
            let &(timeline_id, offset) = entry.key();
            let &idx = entry.value();
            let data = &sealed.buffer[idx as usize];
            if let Ok(event) = Event::decode(data.as_slice()) {
                let (byte_offset, length) = writer.write_entry(&event).await?;
                index_entries.push((
                    (timeline_id, offset),
                    IndexEntry {
                        segment_id,
                        byte_offset,
                        length,
                    },
                ));
            }
        }

        let size = writer.size();
        let entry_count = writer.entry_count();
        writer.finish().await?;

        self.segment_manager
            .update_meta(segment_id, size, entry_count);
        self.index.put_index_batch(&index_entries)?;
        self.write_cache.clear_sealed();

        if let Some(ref wal) = self.wal {
            let current_seg = wal.current_segment_id().await;
            let cp = WalCheckpoint::new(current_seg);
            if let Err(e) = checkpoint::write_checkpoint(&self.index, &cp) {
                warn!(error = ?e, "failed to write WAL checkpoint");
            } else {
                match wal.trim(current_seg).await {
                    Ok(trimmed) if trimmed > 0 => {
                        info!(
                            trimmed,
                            checkpoint_segment = current_seg,
                            "wal segments trimmed"
                        );
                    }
                    Err(e) => {
                        warn!(error = ?e, "failed to trim WAL segments");
                    }
                    _ => {}
                }
            }
        }

        info!(
            segment_id = segment_id,
            entries = entries_count,
            "L1 flush complete"
        );

        Ok(())
    }
}
