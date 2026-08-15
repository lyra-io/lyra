pub mod error;

use crate::Offset;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use error::XunitClientError;

pub type DatasetName = String;
pub type PartitionId = String;
pub type SchemaId = i64;
pub type SnapshotId = String;

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

#[async_trait]
pub trait XunitClient: Send + Sync {
    async fn append_rows(
        &self,
        request: AppendRowsRequest,
    ) -> Result<AppendRowsResponse, XunitClientError>;

    async fn scan(&self, request: ScanRequest) -> Result<ScanResponse, XunitClientError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendRowsRequest {
    pub dataset: DatasetName,
    pub partition: PartitionId,
    pub schema_id: SchemaId,
    pub offset_range: OffsetRange,
    pub rows: Vec<RowData>,
}

impl AppendRowsRequest {
    pub fn new(
        dataset: impl Into<DatasetName>,
        partition: impl Into<PartitionId>,
        schema_id: SchemaId,
        offset_range: OffsetRange,
        rows: Vec<RowData>,
    ) -> Self {
        Self {
            dataset: dataset.into(),
            partition: partition.into(),
            schema_id,
            offset_range,
            rows,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendRowsResponse {
    pub committed_offset: Offset,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanRequest {
    pub dataset: DatasetName,
    pub projection: Vec<String>,
    pub filters: Vec<ScanFilter>,
    pub limit: Option<usize>,
    pub snapshot: Option<SnapshotId>,
}

impl ScanRequest {
    pub fn all(dataset: impl Into<DatasetName>) -> Self {
        Self {
            dataset: dataset.into(),
            projection: Vec::new(),
            filters: Vec::new(),
            limit: None,
            snapshot: None,
        }
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanResponse {
    pub batches: Vec<RowBatch>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowBatch {
    pub schema_id: SchemaId,
    pub rows: Vec<RowData>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowData {
    pub offset: Offset,
    pub payload: Vec<u8>,
}

impl RowData {
    pub fn new(offset: Offset, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            offset,
            payload: payload.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanFilter {
    pub field: String,
    pub op: ScanFilterOp,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScanFilterOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}
