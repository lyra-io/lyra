use serde::{Deserialize, Serialize};

pub type DatasetName = String;
pub type DatasetId = i64;
pub type SchemaId = i64;
pub type SchemaVersion = i64;
pub type PartitionId = String;
pub type Offset = i64;
pub type TimestampMillis = i64;
pub type SnapshotId = String;
pub type FileId = String;
pub type ObjectPath = String;
pub type ActionId = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffsetRange {
    pub start: Offset,
    pub end: Offset,
}

impl OffsetRange {
    pub fn new(start: Offset, end: Offset) -> Self {
        Self { start, end }
    }
}
