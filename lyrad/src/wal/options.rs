use crate::segment::IoMode;
use std::path::PathBuf;
use std::time::Duration;

const MIB: u64 = 1024 * 1024;

/// Configuration for opening a [`SegmentWal`](super::SegmentWal).
#[derive(Debug, Clone)]
pub struct WalOptions {
    /// Directory where segment files are stored and recovered from.
    pub dir: PathBuf,
    /// I/O mode used for segment reads and writes.
    pub io_mode: IoMode,
    /// Maximum size in bytes of a segment file before the WAL rotates to a
    /// new one. A single record may exceed this limit.
    pub max_segment_size: u64,
    /// Maximum number of appends coalesced into one batch write.
    pub batch_max_records: usize,
    /// Maximum payload bytes coalesced into one batch write.
    pub batch_max_bytes: usize,
    /// How long the batcher waits to coalesce more appends before flushing.
    pub batch_linger: Duration,
    /// Capacity of the inbound append queue; bounds memory under bursts.
    pub queue_capacity: usize,
}

impl WalOptions {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            ..Self::default()
        }
    }

    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.max_segment_size == 0 {
            return Err("max_segment_size must be greater than zero");
        }
        if self.batch_max_records == 0 {
            return Err("batch_max_records must be greater than zero");
        }
        if self.batch_max_bytes == 0 {
            return Err("batch_max_bytes must be greater than zero");
        }
        if self.queue_capacity == 0 {
            return Err("queue_capacity must be greater than zero");
        }
        Ok(())
    }
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("/tmp/lyra-wal"),
            io_mode: IoMode::DirectPreferred,
            max_segment_size: 64 * MIB,
            batch_max_records: 1024,
            batch_max_bytes: MIB as usize,
            batch_linger: Duration::from_micros(200),
            queue_capacity: 4096,
        }
    }
}
