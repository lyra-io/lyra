pub mod error;

use async_trait::async_trait;
use chronicle_catalog::{
    Action, ActionRequest, DatasetName, Offset, OffsetRange, PartitionId, SchemaId, SnapshotId,
    Versioned,
};
use error::XunitClientError;
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait XunitClient: Send + Sync {
    async fn append_rows(
        &self,
        request: AppendRowsRequest,
    ) -> Result<AppendRowsResponse, XunitClientError>;

    async fn scan(&self, request: ScanRequest) -> Result<ScanResponse, XunitClientError>;

    async fn submit_action(
        &self,
        request: ActionRequest,
    ) -> Result<Versioned<Action>, XunitClientError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendRowsRequest {
    pub dataset: DatasetName,
    pub partition: PartitionId,
    pub schema_id: SchemaId,
    pub offset_range: OffsetRange,
    pub rows: Vec<RowData>,
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
