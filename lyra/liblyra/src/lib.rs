use std::time::Duration;

pub mod conn;
pub mod error;
mod error_inner;
pub mod lyra;
pub mod metric;
pub mod stream;
pub mod xunit;

#[derive(Debug, Clone)]
pub struct Event {
    pub offset: Option<i64>,
    pub timestamp: Option<i64>,
    pub payload: Vec<u8>,
    pub key: Option<Vec<u8>>,
    pub txn_id: Option<i64>,
}

impl Event {
    pub fn new(payload: Vec<u8>) -> Self {
        Self {
            offset: None,
            timestamp: None,
            payload,
            key: None,
            txn_id: None,
        }
    }

    pub fn with_key(mut self, key: Vec<u8>) -> Self {
        self.key = Some(key);
        self
    }

    pub fn with_txn_id(mut self, txn_id: i64) -> Self {
        self.txn_id = Some(txn_id);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(pub i64);

pub use stream::cursor::EventStream;

#[derive(Debug, Clone)]
pub enum StartPosition {
    Earliest,
    Latest,
    Offset(i64),
    Index { name: String, value: String },
}

#[derive(Debug, Clone)]
pub struct ReadOptions {
    pub(crate) start: StartPosition,
    pub(crate) limit: Option<usize>,
    pub(crate) timeout: Option<std::time::Duration>,
}

impl ReadOptions {
    pub fn earliest() -> Self {
        Self {
            start: StartPosition::Earliest,
            limit: None,
            timeout: None,
        }
    }

    pub fn latest() -> Self {
        Self {
            start: StartPosition::Latest,
            limit: None,
            timeout: None,
        }
    }

    pub fn offset(offset: i64) -> Self {
        Self {
            start: StartPosition::Offset(offset),
            limit: None,
            timeout: None,
        }
    }

    pub fn index(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            start: StartPosition::Index {
                name: name.into(),
                value: value.into(),
            },
            limit: None,
            timeout: None,
        }
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

const DEFAULT_REPLICATION_FACTOR: usize = 3;
const DEFAULT_BATCH_SIZE: usize = 256;
const DEFAULT_MAX_INFLIGHT: usize = 1024;
const DEFAULT_LINGER: Duration = Duration::from_millis(5);

#[derive(Debug, Clone)]
pub struct StreamOptions {
    pub(crate) retention: Option<Duration>,
    pub(crate) compaction: bool,
    pub(crate) replication_factor: usize,
    pub(crate) schema_id: Option<String>,
    pub(crate) max_batch_size: usize,
    pub(crate) max_inflight: usize,
    pub(crate) linger: Duration,
    pub(crate) request_timeout: Duration,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            retention: None,
            compaction: false,
            replication_factor: DEFAULT_REPLICATION_FACTOR,
            schema_id: None,
            max_batch_size: DEFAULT_BATCH_SIZE,
            max_inflight: DEFAULT_MAX_INFLIGHT,
            linger: DEFAULT_LINGER,
            request_timeout: Duration::from_secs(30),
        }
    }
}

impl StreamOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn retention(mut self, duration: Duration) -> Self {
        self.retention = Some(duration);
        self
    }

    pub fn compaction(mut self, enabled: bool) -> Self {
        self.compaction = enabled;
        self
    }

    pub fn replication_factor(mut self, rf: usize) -> Self {
        self.replication_factor = rf;
        self
    }

    pub fn schema_id(mut self, id: String) -> Self {
        self.schema_id = Some(id);
        self
    }

    pub fn max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }

    pub fn max_inflight(mut self, size: usize) -> Self {
        self.max_inflight = size;
        self
    }

    pub fn linger(mut self, duration: std::time::Duration) -> Self {
        self.linger = duration;
        self
    }
}
