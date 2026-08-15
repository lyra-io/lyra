use async_trait::async_trait;
use liboxia::client::{
    DeleteOption, GetOption, GetSequenceUpdatesOption, OxiaClient, PutOption, RangeScanOption,
};
use liboxia::client_builder::OxiaClientBuilder;
use liboxia::errors::OxiaError;
use crate::proto::pb_catalog::{Segment, StreamMeta, UnitInfo, UnitRegistration, UnitStatus};
use prost::Message;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Receiver;
use tracing::{debug, info};

use crate::error::MetadataError;
use crate::{
    Action, ActionId, ActionRequest, ActionStatus, Metadata, Dataset, DatasetName, Versioned,
};

const KEY_PREFIX: &str = "/lyra/streams/";
const UNITS_PREFIX: &str = "/lyra/units/";
const UNITS_MAX: &str = "/lyra/units0"; // '0' > '/' in ASCII
const DATASETS_PREFIX: &str = "/lyra/datasets/";
const DATASET_PARTITION_KEY: &str = "/lyra/datasets";
const DATASET_INDEX_KEY: &str = "/lyra/dataset_index";
const ACTIONS_PREFIX: &str = "/lyra/actions/";
const ACTION_PARTITION_KEY: &str = "/lyra/actions";

pub struct OxiaMetadata {
    client: OxiaClient,
    next_stream_id: AtomicI64,
    next_action_id: AtomicI64,
}

impl OxiaMetadata {
    pub async fn new(service_address: String, namespace: String) -> Result<Self, MetadataError> {
        let client = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            OxiaClientBuilder::new()
                .service_address(service_address)
                .namespace(namespace)
                .build(),
        )
        .await
        .map_err(|_| MetadataError::Transport("oxia client build timed out after 30s".into()))?
        .map_err(|e| MetadataError::Transport(e.to_string()))?;
        let metadata = Self {
            client,
            next_stream_id: AtomicI64::new(1),
            next_action_id: AtomicI64::new(1),
        };
        if let Ok(streams) = metadata.list_streams().await {
            let max_id = streams.iter().map(|t| t.stream_id).max().unwrap_or(0);
            metadata.next_stream_id.store(max_id + 1, Ordering::SeqCst);
        }
        Ok(metadata)
    }

    fn meta_key(name: &str) -> String {
        format!("{}{}", KEY_PREFIX, name)
    }

    fn dataset_key(name: &str) -> String {
        format!("{}{}", DATASETS_PREFIX, name)
    }

    fn action_key(id: &str) -> String {
        format!("{}{}", ACTIONS_PREFIX, id)
    }

    fn dataset_put_options(expected_version: Option<i64>) -> Vec<PutOption> {
        let mut options = vec![PutOption::PartitionKey(DATASET_PARTITION_KEY.to_string())];
        if let Some(expected_version) = expected_version {
            options.push(PutOption::ExpectVersionId(expected_version));
        }
        options
    }

    fn dataset_get_options() -> Vec<GetOption> {
        vec![
            GetOption::PartitionKey(DATASET_PARTITION_KEY.to_string()),
            GetOption::IncludeValue(),
        ]
    }

    fn dataset_delete_options(expected_version: i64) -> Vec<DeleteOption> {
        vec![
            DeleteOption::PartitionKey(DATASET_PARTITION_KEY.to_string()),
            DeleteOption::ExpectVersionId(expected_version),
        ]
    }

    fn action_put_options(expected_version: Option<i64>) -> Vec<PutOption> {
        let mut options = vec![PutOption::PartitionKey(ACTION_PARTITION_KEY.to_string())];
        if let Some(expected_version) = expected_version {
            options.push(PutOption::ExpectVersionId(expected_version));
        }
        options
    }

    fn action_get_options() -> Vec<GetOption> {
        vec![
            GetOption::PartitionKey(ACTION_PARTITION_KEY.to_string()),
            GetOption::IncludeValue(),
        ]
    }

    fn action_scan_options() -> Vec<RangeScanOption> {
        vec![RangeScanOption::PartitionKey(
            ACTION_PARTITION_KEY.to_string(),
        )]
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default()
    }

    fn encode_dataset(dataset: &Dataset) -> Result<Vec<u8>, MetadataError> {
        serde_json::to_vec(dataset)
            .map_err(|error| MetadataError::Internal(format!("failed to encode dataset: {error}")))
    }

    fn encode_dataset_index(datasets: &[DatasetName]) -> Result<Vec<u8>, MetadataError> {
        serde_json::to_vec(datasets).map_err(|error| {
            MetadataError::Internal(format!("failed to encode dataset index: {error}"))
        })
    }

    fn decode_dataset(value: &[u8], version_id: i64) -> Result<Dataset, MetadataError> {
        let mut dataset: Dataset = serde_json::from_slice(value).map_err(|error| {
            MetadataError::Internal(format!("failed to decode dataset: {error}"))
        })?;
        dataset.version = version_id;
        Ok(dataset)
    }

    fn decode_dataset_index(value: &[u8]) -> Result<Vec<DatasetName>, MetadataError> {
        serde_json::from_slice(value).map_err(|error| {
            MetadataError::Internal(format!("failed to decode dataset index: {error}"))
        })
    }

    fn encode_action(action: &Action) -> Result<Vec<u8>, MetadataError> {
        serde_json::to_vec(action)
            .map_err(|error| MetadataError::Internal(format!("failed to encode action: {error}")))
    }

    fn decode_action(value: &[u8], version_id: i64) -> Result<Action, MetadataError> {
        let mut action: Action = serde_json::from_slice(value)
            .map_err(|error| MetadataError::Internal(format!("failed to decode action: {error}")))?;
        action.version = version_id;
        Ok(action)
    }

    fn next_action_id(&self) -> ActionId {
        let sequence = self.next_action_id.fetch_add(1, Ordering::SeqCst);
        format!("action-{}-{}", Self::now_ms(), sequence)
    }

    fn decode_meta(value: &[u8], version_id: i64) -> Result<StreamMeta, MetadataError> {
        let mut meta = StreamMeta::decode(value)
            .map_err(|e| MetadataError::Internal(format!("failed to decode stream: {}", e)))?;
        meta.version = version_id;
        Ok(meta)
    }

    /// Build the key for a unit: /lyra/units/{zone}/{sanitized_address}
    fn unit_key(registration: &UnitRegistration) -> String {
        let zone = if registration.zone.is_empty() {
            "default"
        } else {
            &registration.zone
        };
        let address = &registration
            .unit
            .as_ref()
            .expect("unit info required")
            .address;
        let unit_id = Self::sanitize_address(address);
        format!("{}{}/{}", UNITS_PREFIX, zone, unit_id)
    }

    /// Build the key for unregister by address: scan to find the key.
    fn unit_key_for_address(zone: &str, address: &str) -> String {
        let zone = if zone.is_empty() { "default" } else { zone };
        let unit_id = Self::sanitize_address(address);
        format!("{}{}/{}", UNITS_PREFIX, zone, unit_id)
    }

    fn sanitize_address(address: &str) -> String {
        address.replace("://", "_").replace(['/', ':'], "_")
    }
}

impl OxiaMetadata {
    pub async fn create_dataset(
        &self,
        mut dataset: Dataset,
    ) -> Result<Versioned<Dataset>, MetadataError> {
        let now = Self::now_ms();
        dataset.version = 0;
        dataset.created_at_ms = now;
        dataset.updated_at_ms = now;
        let key = Self::dataset_key(&dataset.name);
        let value = Self::encode_dataset(&dataset)?;

        let result = self
            .client
            .put_with_options(key, value, Self::dataset_put_options(Some(-1)))
            .await
            .map_err(|error| match error {
                OxiaError::UnexpectedVersionId() => {
                    MetadataError::AlreadyExists(dataset.name.clone())
                }
                other => MetadataError::from(other),
            })?;

        dataset.version = result.version.version_id;
        self.add_dataset_to_index(&dataset.name).await?;
        Ok(Versioned::new(dataset, result.version.version_id))
    }

    pub async fn update_dataset(
        &self,
        mut dataset: Dataset,
        expected_version: i64,
    ) -> Result<Versioned<Dataset>, MetadataError> {
        dataset.updated_at_ms = Self::now_ms();
        let key = Self::dataset_key(&dataset.name);
        let value = Self::encode_dataset(&dataset)?;

        let result = self
            .client
            .put_with_options(
                key,
                value,
                Self::dataset_put_options(Some(expected_version)),
            )
            .await
            .map_err(|error| match error {
                OxiaError::UnexpectedVersionId() => MetadataError::VersionConflict {
                    expected: expected_version,
                    actual: -1,
                },
                OxiaError::KeyNotFound() => MetadataError::NotFound(dataset.name.clone()),
                other => MetadataError::from(other),
            })?;

        dataset.version = result.version.version_id;
        Ok(Versioned::new(dataset, result.version.version_id))
    }

    pub async fn get_dataset(&self, name: &str) -> Result<Versioned<Dataset>, MetadataError> {
        let result = self
            .client
            .get_with_options(Self::dataset_key(name), Self::dataset_get_options())
            .await
            .map_err(|error| match error {
                OxiaError::KeyNotFound() => MetadataError::NotFound(name.to_string()),
                other => MetadataError::from(other),
            })?;

        let value = result
            .value
            .ok_or_else(|| MetadataError::NotFound(name.to_string()))?;
        let dataset = Self::decode_dataset(&value, result.version.version_id)?;
        Ok(Versioned::new(dataset, result.version.version_id))
    }

    pub async fn list_datasets(&self) -> Result<Vec<Versioned<Dataset>>, MetadataError> {
        let (names, _) = self.get_dataset_index().await?;
        let mut datasets = Vec::with_capacity(names.len());
        for name in names {
            match self.get_dataset(&name).await {
                Ok(dataset) => datasets.push(dataset),
                Err(MetadataError::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(datasets)
    }

    pub async fn delete_dataset(
        &self,
        name: &str,
        expected_version: i64,
    ) -> Result<(), MetadataError> {
        self.client
            .delete_with_options(
                Self::dataset_key(name),
                Self::dataset_delete_options(expected_version),
            )
            .await
            .map_err(|error| match error {
                OxiaError::KeyNotFound() => MetadataError::NotFound(name.to_string()),
                OxiaError::UnexpectedVersionId() => MetadataError::VersionConflict {
                    expected: expected_version,
                    actual: -1,
                },
                other => MetadataError::from(other),
            })?;
        self.remove_dataset_from_index(name).await?;
        Ok(())
    }

    async fn get_dataset_index(&self) -> Result<(Vec<DatasetName>, i64), MetadataError> {
        match self
            .client
            .get_with_options(DATASET_INDEX_KEY.to_string(), Self::dataset_get_options())
            .await
        {
            Ok(result) => {
                let value = result.value.unwrap_or_default();
                Ok((
                    Self::decode_dataset_index(&value)?,
                    result.version.version_id,
                ))
            }
            Err(OxiaError::KeyNotFound()) => Ok((Vec::new(), -1)),
            Err(error) => Err(MetadataError::from(error)),
        }
    }

    async fn put_dataset_index(
        &self,
        names: &[DatasetName],
        _expected_version: i64,
    ) -> Result<(), MetadataError> {
        self.client
            .put_with_options(
                DATASET_INDEX_KEY.to_string(),
                Self::encode_dataset_index(names)?,
                Self::dataset_put_options(None),
            )
            .await
            .map(|_| ())
            .map_err(MetadataError::from)
    }

    async fn add_dataset_to_index(&self, name: &str) -> Result<(), MetadataError> {
        for _ in 0..8 {
            let (mut names, version) = self.get_dataset_index().await?;
            if names.iter().any(|existing| existing == name) {
                return Ok(());
            }
            names.push(name.to_string());
            names.sort();
            match self.put_dataset_index(&names, version).await {
                Ok(()) => return Ok(()),
                Err(MetadataError::VersionConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(MetadataError::Internal(
            "failed to update dataset index after retries".into(),
        ))
    }

    async fn remove_dataset_from_index(&self, name: &str) -> Result<(), MetadataError> {
        for _ in 0..8 {
            let (mut names, version) = self.get_dataset_index().await?;
            let before = names.len();
            names.retain(|existing| existing != name);
            if names.len() == before {
                return Ok(());
            }
            match self.put_dataset_index(&names, version).await {
                Ok(()) => return Ok(()),
                Err(MetadataError::VersionConflict { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(MetadataError::Internal(
            "failed to update dataset index after retries".into(),
        ))
    }

    pub async fn submit_action(
        &self,
        request: ActionRequest,
    ) -> Result<Versioned<Action>, MetadataError> {
        let now = Self::now_ms();
        let mut action = Action {
            id: self.next_action_id(),
            request,
            status: ActionStatus::Pending,
            message: None,
            version: 0,
            created_at_ms: now,
            updated_at_ms: now,
        };
        let key = Self::action_key(&action.id);
        let value = Self::encode_action(&action)?;
        let result = self
            .client
            .put_with_options(key, value, Self::action_put_options(Some(-1)))
            .await
            .map_err(MetadataError::from)?;

        action.version = result.version.version_id;
        Ok(Versioned::new(action, result.version.version_id))
    }

    pub async fn get_action(&self, id: &ActionId) -> Result<Versioned<Action>, MetadataError> {
        let result = self
            .client
            .get_with_options(Self::action_key(id), Self::action_get_options())
            .await
            .map_err(|error| match error {
                OxiaError::KeyNotFound() => MetadataError::NotFound(id.clone()),
                other => MetadataError::from(other),
            })?;
        let value = result
            .value
            .ok_or_else(|| MetadataError::NotFound(id.clone()))?;
        let action = Self::decode_action(&value, result.version.version_id)?;
        Ok(Versioned::new(action, result.version.version_id))
    }

    pub async fn list_actions(
        &self,
        dataset: Option<&DatasetName>,
    ) -> Result<Vec<Versioned<Action>>, MetadataError> {
        let result = self
            .client
            .range_scan_with_options(
                ACTIONS_PREFIX.to_string(),
                format!("{}\x7f", ACTIONS_PREFIX),
                Self::action_scan_options(),
            )
            .await
            .map_err(MetadataError::from)?;

        let mut actions = Vec::with_capacity(result.records.len());
        for entry in &result.records {
            if let Some(ref value) = entry.value {
                let action = Self::decode_action(value, entry.version.version_id)?;
                if dataset
                    .map(|dataset| action.request.dataset == *dataset)
                    .unwrap_or(true)
                {
                    actions.push(Versioned::new(action, entry.version.version_id));
                }
            }
        }
        Ok(actions)
    }

    pub async fn get_stream(&self, name: &str) -> Result<StreamMeta, MetadataError> {
        let key = Self::meta_key(name);
        debug!("get_stream: key={}", key);

        let result = self
            .client
            .get_with_options(key, vec![GetOption::IncludeValue()])
            .await
            .map_err(|e| match e {
                OxiaError::KeyNotFound() => MetadataError::NotFound(name.to_string()),
                other => MetadataError::from(other),
            })?;

        let value = result
            .value
            .ok_or_else(|| MetadataError::NotFound(name.to_string()))?;
        Self::decode_meta(&value, result.version.version_id)
    }

    pub async fn stream_update(
        &self,
        meta: &StreamMeta,
        expected_version: i64,
    ) -> Result<StreamMeta, MetadataError> {
        let key = Self::meta_key(&meta.name);
        let value = meta.encode_to_vec();

        let result = self
            .client
            .put_with_options(
                key,
                value,
                vec![PutOption::ExpectVersionId(expected_version)],
            )
            .await
            .map_err(|e| match e {
                OxiaError::UnexpectedVersionId() => MetadataError::VersionConflict {
                    expected: expected_version,
                    actual: -1,
                },
                other => MetadataError::from(other),
            })?;

        let mut updated = meta.clone();
        updated.version = result.version.version_id;
        Ok(updated)
    }

    pub async fn create_stream(&self, name: &str) -> Result<StreamMeta, MetadataError> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::SeqCst);
        let meta = StreamMeta {
            name: name.to_string(),
            stream_id,
            status: crate::proto::pb_catalog::StreamStatus::Active as i32,
            term: 0,
            lra: 0,
            version: 0,
        };
        let key = Self::meta_key(name);
        let value = meta.encode_to_vec();

        let result = self
            .client
            .put_with_options(key, value, vec![PutOption::ExpectVersionId(-1)])
            .await
            .map_err(|e| match e {
                OxiaError::UnexpectedVersionId() => MetadataError::AlreadyExists(name.to_string()),
                other => MetadataError::from(other),
            })?;

        let mut created = meta;
        created.version = result.version.version_id;
        Ok(created)
    }

    pub async fn delete_stream(
        &self,
        name: &str,
        expected_version: i64,
    ) -> Result<(), MetadataError> {
        let key = Self::meta_key(name);
        self.client
            .delete_with_options(
                key,
                vec![liboxia::client::DeleteOption::ExpectVersionId(
                    expected_version,
                )],
            )
            .await
            .map_err(|e| match e {
                OxiaError::KeyNotFound() => MetadataError::NotFound(name.to_string()),
                OxiaError::UnexpectedVersionId() => MetadataError::VersionConflict {
                    expected: expected_version,
                    actual: -1,
                },
                other => MetadataError::from(other),
            })?;
        // TODO: also delete vfs keys
        Ok(())
    }

    pub async fn list_streams(&self) -> Result<Vec<StreamMeta>, MetadataError> {
        let min_key = KEY_PREFIX.to_string();
        let max_key = format!("{}\x7f", KEY_PREFIX);

        let result = self
            .client
            .range_scan(min_key, max_key)
            .await
            .map_err(MetadataError::from)?;

        let mut streams = Vec::with_capacity(result.records.len());
        for entry in &result.records {
            // Skip vfs keys (contain /seg-)
            if entry.key.contains("/seg-") {
                continue;
            }
            if let Some(ref value) = entry.value {
                let meta = Self::decode_meta(value, entry.version.version_id)?;
                streams.push(meta);
            }
        }
        Ok(streams)
    }

    pub async fn put_segment(
        &self,
        stream_name: &str,
        segment: &Segment,
        expected_version: i64,
    ) -> Result<Versioned<Segment>, MetadataError> {
        let key = crate::segment_key(stream_name, segment.start_offset);
        let value = segment.encode_to_vec();
        let result = self
            .client
            .put_with_options(
                key,
                value,
                vec![PutOption::ExpectVersionId(expected_version)],
            )
            .await
            .map_err(|e| match e {
                OxiaError::UnexpectedVersionId() => MetadataError::VersionConflict {
                    expected: expected_version,
                    actual: -1,
                },
                other => MetadataError::from(other),
            })?;

        Ok(Versioned::new(segment.clone(), result.version.version_id))
    }

    pub async fn list_segments(
        &self,
        stream_name: &str,
    ) -> Result<Vec<Versioned<Segment>>, MetadataError> {
        let min_key = crate::segment_key_prefix(stream_name);
        let max_key = crate::segment_key_max(stream_name);

        let result = self
            .client
            .range_scan(min_key, max_key)
            .await
            .map_err(MetadataError::from)?;

        let mut segments = Vec::with_capacity(result.records.len());
        for entry in &result.records {
            if let Some(ref value) = entry.value {
                let seg = Segment::decode(value.as_slice())
                    .map_err(|e| MetadataError::Internal(format!("failed to decode vfs: {}", e)))?;
                segments.push(Versioned::new(seg, entry.version.version_id));
            }
        }
        Ok(segments)
    }

    pub async fn get_last_segment(
        &self,
        stream_name: &str,
    ) -> Result<Option<Versioned<Segment>>, MetadataError> {
        let segments = self.list_segments(stream_name).await?;
        Ok(segments.into_iter().last())
    }

    /// Get the vfs that covers a given offset (floor lookup).
    ///
    /// Scans segments with `start_offset <= offset` and returns the last one
    /// (the vfs with the largest start_offset that doesn't exceed `offset`).
    pub async fn get_segment_for_offset(
        &self,
        stream_name: &str,
        offset: i64,
    ) -> Result<Option<Versioned<Segment>>, MetadataError> {
        let min_key = crate::segment_key_prefix(stream_name);
        // Exclusive upper bound: offset + 1 so we include seg-{offset} itself.
        let max_key = crate::segment_key(stream_name, offset + 1);

        let result = self
            .client
            .range_scan(min_key, max_key)
            .await
            .map_err(MetadataError::from)?;

        // Take the last entry: the vfs with the largest start_offset <= offset.
        if let Some(entry) = result.records.last()
            && let Some(ref value) = entry.value
        {
            let seg = Segment::decode(value.as_slice())
                .map_err(|e| MetadataError::Internal(format!("failed to decode vfs: {}", e)))?;
            return Ok(Some(Versioned::new(seg, entry.version.version_id)));
        }
        Ok(None)
    }

    /// Get an existing stream or create a new one if it doesn't exist.
    ///
    /// Uses `ExpectVersionId(-1)` for creation so that concurrent callers
    /// race safely — the loser sees `AlreadyExists` and falls back to get.
    pub async fn stream_get_or_insert(&self, name: &str) -> Result<StreamMeta, MetadataError> {
        match self.get_stream(name).await {
            Ok(tc) => Ok(tc),
            Err(MetadataError::NotFound(_)) => match self.create_stream(name).await {
                Ok(tc) => Ok(tc),
                Err(MetadataError::AlreadyExists(_)) => self.get_stream(name).await,
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        }
    }

    /// Get the writable (last) vfs for a stream, or create one.
    ///
    /// The `ensemble_supplier` is only called when no vfs exists — it
    /// should select the ensemble (e.g. via `select_ensemble`).
    ///
    /// Uses `ExpectVersionId(-1)` for creation so concurrent callers race
    /// safely — the loser sees `VersionConflict` and falls back to get.
    pub async fn stream_get_or_init_last_segment<F, Fut>(
        &self,
        stream_name: &str,
        ensemble_supplier: F,
    ) -> Result<Versioned<Segment>, MetadataError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<UnitInfo>, MetadataError>>,
    {
        if let Some(last) = self.get_last_segment(stream_name).await? {
            return Ok(last);
        }
        let ensemble = ensemble_supplier().await?;
        let segment = Segment {
            ensemble,
            start_offset: 1,
        };
        match self.put_segment(stream_name, &segment, -1).await {
            Ok(vs) => Ok(vs),
            Err(MetadataError::VersionConflict { .. }) => self
                .get_last_segment(stream_name)
                .await?
                .ok_or_else(|| MetadataError::Internal("vfs vanished after conflict".into())),
            Err(e) => Err(e),
        }
    }

    /// Get or create a stream, then atomically bump its term.
    ///
    /// Combines `stream_get_or_insert` + term increment in a single method
    /// to avoid a redundant read. Retries on CAS conflict.
    pub async fn stream_new_term(&self, name: &str) -> Result<StreamMeta, MetadataError> {
        let mut tc = self.stream_get_or_insert(name).await?;
        loop {
            let mut updated = tc.clone();
            updated.term = tc.term + 1;
            match self.stream_update(&updated, tc.version).await {
                Ok(tc) => return Ok(tc),
                Err(MetadataError::VersionConflict { .. }) => {
                    tc = self.get_stream(name).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Register a unit at /lyra/units/{zone}/{unit-id}.
    /// Each unit has its own key — no CAS contention between units.
    pub async fn register_unit(&self, registration: &UnitRegistration) -> Result<(), MetadataError> {
        let key = Self::unit_key(registration);
        let value = registration.encode_to_vec();
        let address = &registration
            .unit
            .as_ref()
            .expect("unit info required")
            .address;
        info!(address = %address, zone = %registration.zone, key = %key, "register_unit");

        self.client
            .put(key, value)
            .await
            .map_err(MetadataError::from)?;

        Ok(())
    }

    /// Unregister a unit by deleting its key.
    pub async fn unregister_unit(&self, address: &str, zone: &str) -> Result<(), MetadataError> {
        let key = Self::unit_key_for_address(zone, address);
        self.client.delete(key).await.map_err(|e| match e {
            OxiaError::KeyNotFound() => MetadataError::NotFound(address.to_string()),
            other => MetadataError::from(other),
        })?;
        Ok(())
    }

    /// List all registered units across all zones via range scan.
    pub async fn list_units(&self) -> Result<Vec<UnitRegistration>, MetadataError> {
        let result = self
            .client
            .range_scan(UNITS_PREFIX.to_string(), UNITS_MAX.to_string())
            .await
            .map_err(MetadataError::from)?;

        let mut units = Vec::with_capacity(result.records.len());
        for entry in &result.records {
            if let Some(ref value) = entry.value {
                let reg = UnitRegistration::decode(value.as_slice())
                    .map_err(|e| MetadataError::Internal(format!("failed to decode unit: {}", e)))?;
                units.push(reg);
            }
        }
        Ok(units)
    }

    /// List only writable units.
    pub async fn list_writable_units(&self) -> Result<Vec<UnitRegistration>, MetadataError> {
        let units = self.list_units().await?;
        Ok(units
            .into_iter()
            .filter(|u| u.status() == UnitStatus::Writable)
            .collect())
    }

    /// Subscribe to vfs key updates for a stream.
    ///
    /// Uses Oxia sequence key subscription to receive the highest vfs key
    /// each time a new vfs is written. The receiver yields the full key
    /// string (e.g. `/lyra/streams/{name}/seg-0000000000000000001`).
    pub async fn subscribe_segments(
        &self,
        stream_name: &str,
    ) -> Result<Receiver<String>, MetadataError> {
        let key = crate::segment_key_prefix(stream_name);
        let partition_key = stream_name.to_string();
        self.client
            .get_sequence_updates_with_options(
                key,
                vec![GetSequenceUpdatesOption::PartitionKey(partition_key)],
            )
            .await
            .map_err(MetadataError::from)
    }
}

#[async_trait]
impl Metadata for OxiaMetadata {
    async fn create_dataset(&self, dataset: Dataset) -> Result<Versioned<Dataset>, MetadataError> {
        OxiaMetadata::create_dataset(self, dataset).await
    }

    async fn update_dataset(
        &self,
        dataset: Dataset,
        expected_version: i64,
    ) -> Result<Versioned<Dataset>, MetadataError> {
        OxiaMetadata::update_dataset(self, dataset, expected_version).await
    }

    async fn get_dataset(&self, name: &str) -> Result<Versioned<Dataset>, MetadataError> {
        OxiaMetadata::get_dataset(self, name).await
    }

    async fn list_datasets(&self) -> Result<Vec<Versioned<Dataset>>, MetadataError> {
        OxiaMetadata::list_datasets(self).await
    }

    async fn delete_dataset(&self, name: &str, expected_version: i64) -> Result<(), MetadataError> {
        OxiaMetadata::delete_dataset(self, name, expected_version).await
    }

    async fn submit_action(
        &self,
        request: ActionRequest,
    ) -> Result<Versioned<Action>, MetadataError> {
        OxiaMetadata::submit_action(self, request).await
    }

    async fn get_action(&self, id: &ActionId) -> Result<Versioned<Action>, MetadataError> {
        OxiaMetadata::get_action(self, id).await
    }

    async fn list_actions(
        &self,
        dataset: Option<&DatasetName>,
    ) -> Result<Vec<Versioned<Action>>, MetadataError> {
        OxiaMetadata::list_actions(self, dataset).await
    }

    async fn get_stream(&self, name: &str) -> Result<StreamMeta, MetadataError> {
        OxiaMetadata::get_stream(self, name).await
    }

    async fn stream_update(
        &self,
        meta: &StreamMeta,
        expected_version: i64,
    ) -> Result<StreamMeta, MetadataError> {
        OxiaMetadata::stream_update(self, meta, expected_version).await
    }

    async fn create_stream(&self, name: &str) -> Result<StreamMeta, MetadataError> {
        OxiaMetadata::create_stream(self, name).await
    }

    async fn delete_stream(&self, name: &str, expected_version: i64) -> Result<(), MetadataError> {
        OxiaMetadata::delete_stream(self, name, expected_version).await
    }

    async fn list_streams(&self) -> Result<Vec<StreamMeta>, MetadataError> {
        OxiaMetadata::list_streams(self).await
    }

    async fn put_segment(
        &self,
        stream_name: &str,
        segment: &Segment,
        expected_version: i64,
    ) -> Result<Versioned<Segment>, MetadataError> {
        OxiaMetadata::put_segment(self, stream_name, segment, expected_version).await
    }

    async fn list_segments(
        &self,
        stream_name: &str,
    ) -> Result<Vec<Versioned<Segment>>, MetadataError> {
        OxiaMetadata::list_segments(self, stream_name).await
    }

    async fn get_last_segment(
        &self,
        stream_name: &str,
    ) -> Result<Option<Versioned<Segment>>, MetadataError> {
        OxiaMetadata::get_last_segment(self, stream_name).await
    }

    async fn get_segment_for_offset(
        &self,
        stream_name: &str,
        offset: i64,
    ) -> Result<Option<Versioned<Segment>>, MetadataError> {
        OxiaMetadata::get_segment_for_offset(self, stream_name, offset).await
    }

    async fn stream_get_or_insert(&self, name: &str) -> Result<StreamMeta, MetadataError> {
        OxiaMetadata::stream_get_or_insert(self, name).await
    }

    async fn stream_new_term(&self, name: &str) -> Result<StreamMeta, MetadataError> {
        OxiaMetadata::stream_new_term(self, name).await
    }

    async fn register_unit(&self, registration: &UnitRegistration) -> Result<(), MetadataError> {
        OxiaMetadata::register_unit(self, registration).await
    }

    async fn unregister_unit(&self, address: &str, zone: &str) -> Result<(), MetadataError> {
        OxiaMetadata::unregister_unit(self, address, zone).await
    }

    async fn list_units(&self) -> Result<Vec<UnitRegistration>, MetadataError> {
        OxiaMetadata::list_units(self).await
    }

    async fn list_writable_units(&self) -> Result<Vec<UnitRegistration>, MetadataError> {
        OxiaMetadata::list_writable_units(self).await
    }

    async fn subscribe_segments(
        &self,
        stream_name: &str,
    ) -> Result<Receiver<String>, MetadataError> {
        OxiaMetadata::subscribe_segments(self, stream_name).await
    }
}
