use crate::segment::IoMode;
use std::path::PathBuf;

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
            queue_capacity: 4096,
        }
    }
}
