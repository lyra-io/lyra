use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::types::{ActionId, DatasetName, SchemaVersion, TimestampMillis};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dataset {
    pub name: DatasetName,
    pub schema: DatasetSchema,
    pub policies: DatasetPolicies,
    pub status: DatasetStatus,
    pub version: i64,
    pub created_at_ms: TimestampMillis,
    pub updated_at_ms: TimestampMillis,
}

impl Dataset {
    pub fn new(name: impl Into<DatasetName>, schema: DatasetSchema) -> Self {
        Self {
            name: name.into(),
            schema,
            policies: DatasetPolicies::default(),
            status: DatasetStatus::Active,
            version: 0,
            created_at_ms: 0,
            updated_at_ms: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetStatus {
    Active,
    Disabled,
    Dropped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetSchema {
    pub version: SchemaVersion,
    pub fields: Vec<DatasetField>,
}

impl DatasetSchema {
    pub fn new(fields: Vec<DatasetField>) -> Self {
        Self { version: 1, fields }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetField {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: BTreeMap<String, String>,
}

impl DatasetField {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataType {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Binary,
    Date,
    Timestamp,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetPolicies {
    pub retention: RetentionPolicy,
    pub storage: StoragePolicy,
    pub indexing: IndexingPolicy,
    pub materialization: MaterializationPolicy,
    pub compaction: CompactionPolicy,
}

impl Default for DatasetPolicies {
    fn default() -> Self {
        Self {
            retention: RetentionPolicy::default(),
            storage: StoragePolicy::default(),
            indexing: IndexingPolicy::default(),
            materialization: MaterializationPolicy::default(),
            compaction: CompactionPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub max_age_ms: Option<i64>,
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePolicy {
    pub row_enabled: bool,
    pub column_enabled: bool,
    pub parquet_enabled: bool,
    pub offload: OffloadPolicy,
}

impl Default for StoragePolicy {
    fn default() -> Self {
        Self {
            row_enabled: true,
            column_enabled: true,
            parquet_enabled: true,
            offload: OffloadPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffloadPolicy {
    pub enabled: bool,
    pub destination: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexingPolicy {
    pub primary_key: Vec<String>,
    pub secondary_indexes: Vec<SecondaryIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecondaryIndex {
    pub name: String,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializationPolicy {
    pub mode: MaterializationMode,
    pub target_freshness_ms: Option<i64>,
}

impl Default for MaterializationPolicy {
    fn default() -> Self {
        Self {
            mode: MaterializationMode::Incremental,
            target_freshness_ms: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationMode {
    None,
    Incremental,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionPolicy {
    pub enabled: bool,
    pub target_file_size_bytes: Option<u64>,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            target_file_size_bytes: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionRequest {
    pub kind: ActionKind,
    pub dataset: DatasetName,
    pub parameters: BTreeMap<String, String>,
}

impl ActionRequest {
    pub fn new(kind: ActionKind, dataset: impl Into<DatasetName>) -> Self {
        Self {
            kind,
            dataset: dataset.into(),
            parameters: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Action {
    pub id: ActionId,
    pub request: ActionRequest,
    pub status: ActionStatus,
    pub message: Option<String>,
    pub version: i64,
    pub created_at_ms: TimestampMillis,
    pub updated_at_ms: TimestampMillis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    Unload,
    Offload,
    Optimize,
    Compact,
    Vacuum,
    Refresh,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Canceled,
}
