pub mod conn;
pub mod error;
mod error_inner;
pub mod metric;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct Offset(pub i64);
