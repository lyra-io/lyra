use std::path::PathBuf;
use std::time::Duration;

const MIB: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    DirectPreferred,
    DirectRequired,
    Standard,
}

#[derive(Debug, Clone)]
pub struct WalOptions {
    pub dir: PathBuf,
    pub io_mode: IoMode,
    pub max_segment_size: u64,
    pub max_record_size: usize,
    pub batch_max_records: usize,
    pub batch_max_bytes: usize,
    pub batch_linger: Duration,
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
        if self.max_record_size == 0 {
            return Err("max_record_size must be greater than zero");
        }
        if self.max_record_size > u32::MAX as usize {
            return Err("max_record_size must fit in u32");
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
            max_record_size: 32 * MIB as usize,
            batch_max_records: 1024,
            batch_max_bytes: MIB as usize,
            batch_linger: Duration::from_micros(200),
            queue_capacity: 4096,
        }
    }
}
