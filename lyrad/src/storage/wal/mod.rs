use crate::option::IoMode;

#[derive(Debug, Clone)]
pub struct WalOptions {
    pub dir: String,
    pub max_segment_size: Option<u64>,
    pub io_mode: IoMode,
}
