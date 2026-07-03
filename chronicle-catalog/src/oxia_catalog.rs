use async_trait::async_trait;
use chronicle_proto::pb_catalog::{Segment, TimelineMeta, UnitInfo, UnitRegistration, UnitStatus};
use liboxia::client::{GetOption, GetSequenceUpdatesOption, OxiaClient, PutOption};
use liboxia::client_builder::OxiaClientBuilder;
use liboxia::errors::OxiaError;
use prost::Message;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::mpsc::Receiver;
use tracing::{debug, info};

use crate::error::CatalogError;
use crate::{Catalog, Versioned};

const KEY_PREFIX: &str = "/chronicle/timelines/";
const UNITS_PREFIX: &str = "/chronicle/units/";
const UNITS_MAX: &str = "/chronicle/units0"; // '0' > '/' in ASCII

pub struct OxiaCatalog {
    client: OxiaClient,
    next_timeline_id: AtomicI64,
}

impl OxiaCatalog {
    pub async fn new(service_address: String, namespace: String) -> Result<Self, CatalogError> {
        let client = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            OxiaClientBuilder::new()
                .service_address(service_address)
                .namespace(namespace)
                .build(),
        )
        .await
        .map_err(|_| CatalogError::Transport("oxia client build timed out after 30s".into()))?
        .map_err(|e| CatalogError::Transport(e.to_string()))?;
        let catalog = Self {
            client,
            next_timeline_id: AtomicI64::new(1),
        };
        if let Ok(timelines) = catalog.list_timelines().await {
            let max_id = timelines.iter().map(|t| t.timeline_id).max().unwrap_or(0);
            catalog.next_timeline_id.store(max_id + 1, Ordering::SeqCst);
        }
        Ok(catalog)
    }

    fn meta_key(name: &str) -> String {
        format!("{}{}", KEY_PREFIX, name)
    }

    fn decode_meta(value: &[u8], version_id: i64) -> Result<TimelineMeta, CatalogError> {
        let mut meta = TimelineMeta::decode(value)
            .map_err(|e| CatalogError::Internal(format!("failed to decode timeline: {}", e)))?;
        meta.version = version_id;
        Ok(meta)
    }

    /// Build the key for a unit: /chronicle/units/{zone}/{sanitized_address}
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

impl OxiaCatalog {
    pub async fn get_timeline(&self, name: &str) -> Result<TimelineMeta, CatalogError> {
        let key = Self::meta_key(name);
        debug!("get_timeline: key={}", key);

        let result = self
            .client
            .get_with_options(key, vec![GetOption::IncludeValue()])
            .await
            .map_err(|e| match e {
                OxiaError::KeyNotFound() => CatalogError::NotFound(name.to_string()),
                other => CatalogError::from(other),
            })?;

        let value = result
            .value
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;
        Self::decode_meta(&value, result.version.version_id)
    }

    pub async fn timeline_update(
        &self,
        meta: &TimelineMeta,
        expected_version: i64,
    ) -> Result<TimelineMeta, CatalogError> {
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
                OxiaError::UnexpectedVersionId() => CatalogError::VersionConflict {
                    expected: expected_version,
                    actual: -1,
                },
                other => CatalogError::from(other),
            })?;

        let mut updated = meta.clone();
        updated.version = result.version.version_id;
        Ok(updated)
    }

    pub async fn create_timeline(&self, name: &str) -> Result<TimelineMeta, CatalogError> {
        let timeline_id = self.next_timeline_id.fetch_add(1, Ordering::SeqCst);
        let meta = TimelineMeta {
            name: name.to_string(),
            timeline_id,
            status: chronicle_proto::pb_catalog::TimelineStatus::Active as i32,
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
                OxiaError::UnexpectedVersionId() => CatalogError::AlreadyExists(name.to_string()),
                other => CatalogError::from(other),
            })?;

        let mut created = meta;
        created.version = result.version.version_id;
        Ok(created)
    }

    pub async fn delete_timeline(
        &self,
        name: &str,
        expected_version: i64,
    ) -> Result<(), CatalogError> {
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
                OxiaError::KeyNotFound() => CatalogError::NotFound(name.to_string()),
                OxiaError::UnexpectedVersionId() => CatalogError::VersionConflict {
                    expected: expected_version,
                    actual: -1,
                },
                other => CatalogError::from(other),
            })?;
        // TODO: also delete vfs keys
        Ok(())
    }

    pub async fn list_timelines(&self) -> Result<Vec<TimelineMeta>, CatalogError> {
        let min_key = KEY_PREFIX.to_string();
        let max_key = format!("{}\x7f", KEY_PREFIX);

        let result = self
            .client
            .range_scan(min_key, max_key)
            .await
            .map_err(CatalogError::from)?;

        let mut timelines = Vec::with_capacity(result.records.len());
        for record in &result.records {
            // Skip vfs keys (contain /seg-)
            if record.key.contains("/seg-") {
                continue;
            }
            if let Some(ref value) = record.value {
                let meta = Self::decode_meta(value, record.version.version_id)?;
                timelines.push(meta);
            }
        }
        Ok(timelines)
    }

    pub async fn put_segment(
        &self,
        timeline_name: &str,
        segment: &Segment,
        expected_version: i64,
    ) -> Result<Versioned<Segment>, CatalogError> {
        let key = crate::segment_key(timeline_name, segment.start_offset);
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
                OxiaError::UnexpectedVersionId() => CatalogError::VersionConflict {
                    expected: expected_version,
                    actual: -1,
                },
                other => CatalogError::from(other),
            })?;

        Ok(Versioned::new(segment.clone(), result.version.version_id))
    }

    pub async fn list_segments(
        &self,
        timeline_name: &str,
    ) -> Result<Vec<Versioned<Segment>>, CatalogError> {
        let min_key = crate::segment_key_prefix(timeline_name);
        let max_key = crate::segment_key_max(timeline_name);

        let result = self
            .client
            .range_scan(min_key, max_key)
            .await
            .map_err(CatalogError::from)?;

        let mut segments = Vec::with_capacity(result.records.len());
        for record in &result.records {
            if let Some(ref value) = record.value {
                let seg = Segment::decode(value.as_slice())
                    .map_err(|e| CatalogError::Internal(format!("failed to decode vfs: {}", e)))?;
                segments.push(Versioned::new(seg, record.version.version_id));
            }
        }
        Ok(segments)
    }

    pub async fn get_last_segment(
        &self,
        timeline_name: &str,
    ) -> Result<Option<Versioned<Segment>>, CatalogError> {
        let segments = self.list_segments(timeline_name).await?;
        Ok(segments.into_iter().last())
    }

    /// Get the vfs that covers a given offset (floor lookup).
    ///
    /// Scans segments with `start_offset <= offset` and returns the last one
    /// (the vfs with the largest start_offset that doesn't exceed `offset`).
    pub async fn get_segment_for_offset(
        &self,
        timeline_name: &str,
        offset: i64,
    ) -> Result<Option<Versioned<Segment>>, CatalogError> {
        let min_key = crate::segment_key_prefix(timeline_name);
        // Exclusive upper bound: offset + 1 so we include seg-{offset} itself.
        let max_key = crate::segment_key(timeline_name, offset + 1);

        let result = self
            .client
            .range_scan(min_key, max_key)
            .await
            .map_err(CatalogError::from)?;

        // Take the last record — the vfs with the largest start_offset <= offset.
        if let Some(record) = result.records.last()
            && let Some(ref value) = record.value
        {
            let seg = Segment::decode(value.as_slice())
                .map_err(|e| CatalogError::Internal(format!("failed to decode vfs: {}", e)))?;
            return Ok(Some(Versioned::new(seg, record.version.version_id)));
        }
        Ok(None)
    }

    /// Get an existing timeline or create a new one if it doesn't exist.
    ///
    /// Uses `ExpectVersionId(-1)` for creation so that concurrent callers
    /// race safely — the loser sees `AlreadyExists` and falls back to get.
    pub async fn tl_fetch_or_insert(&self, name: &str) -> Result<TimelineMeta, CatalogError> {
        match self.get_timeline(name).await {
            Ok(tc) => Ok(tc),
            Err(CatalogError::NotFound(_)) => match self.create_timeline(name).await {
                Ok(tc) => Ok(tc),
                Err(CatalogError::AlreadyExists(_)) => self.get_timeline(name).await,
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        }
    }

    /// Get the writable (last) vfs for a timeline, or create one.
    ///
    /// The `ensemble_supplier` is only called when no vfs exists — it
    /// should select the ensemble (e.g. via `select_ensemble`).
    ///
    /// Uses `ExpectVersionId(-1)` for creation so concurrent callers race
    /// safely — the loser sees `VersionConflict` and falls back to get.
    pub async fn timeline_get_or_init_last_segment<F, Fut>(
        &self,
        timeline_name: &str,
        ensemble_supplier: F,
    ) -> Result<Versioned<Segment>, CatalogError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<UnitInfo>, CatalogError>>,
    {
        if let Some(last) = self.get_last_segment(timeline_name).await? {
            return Ok(last);
        }
        let ensemble = ensemble_supplier().await?;
        let segment = Segment {
            ensemble,
            start_offset: 1,
        };
        match self.put_segment(timeline_name, &segment, -1).await {
            Ok(vs) => Ok(vs),
            Err(CatalogError::VersionConflict { .. }) => self
                .get_last_segment(timeline_name)
                .await?
                .ok_or_else(|| CatalogError::Internal("vfs vanished after conflict".into())),
            Err(e) => Err(e),
        }
    }

    /// Get or create a timeline, then atomically bump its term.
    ///
    /// Combines `tl_fetch_or_insert` + term increment in a single method
    /// to avoid a redundant read. Retries on CAS conflict.
    pub async fn tl_new_term(&self, name: &str) -> Result<TimelineMeta, CatalogError> {
        let mut tc = self.tl_fetch_or_insert(name).await?;
        loop {
            let mut updated = tc.clone();
            updated.term = tc.term + 1;
            match self.timeline_update(&updated, tc.version).await {
                Ok(tc) => return Ok(tc),
                Err(CatalogError::VersionConflict { .. }) => {
                    tc = self.get_timeline(name).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Register a unit at /chronicle/units/{zone}/{unit-id}.
    /// Each unit has its own key — no CAS contention between units.
    pub async fn register_unit(&self, registration: &UnitRegistration) -> Result<(), CatalogError> {
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
            .map_err(CatalogError::from)?;

        Ok(())
    }

    /// Unregister a unit by deleting its key.
    pub async fn unregister_unit(&self, address: &str, zone: &str) -> Result<(), CatalogError> {
        let key = Self::unit_key_for_address(zone, address);
        self.client.delete(key).await.map_err(|e| match e {
            OxiaError::KeyNotFound() => CatalogError::NotFound(address.to_string()),
            other => CatalogError::from(other),
        })?;
        Ok(())
    }

    /// List all registered units across all zones via range scan.
    pub async fn list_units(&self) -> Result<Vec<UnitRegistration>, CatalogError> {
        let result = self
            .client
            .range_scan(UNITS_PREFIX.to_string(), UNITS_MAX.to_string())
            .await
            .map_err(CatalogError::from)?;

        let mut units = Vec::with_capacity(result.records.len());
        for record in &result.records {
            if let Some(ref value) = record.value {
                let reg = UnitRegistration::decode(value.as_slice())
                    .map_err(|e| CatalogError::Internal(format!("failed to decode unit: {}", e)))?;
                units.push(reg);
            }
        }
        Ok(units)
    }

    /// List only writable units.
    pub async fn list_writable_units(&self) -> Result<Vec<UnitRegistration>, CatalogError> {
        let units = self.list_units().await?;
        Ok(units
            .into_iter()
            .filter(|u| u.status() == UnitStatus::Writable)
            .collect())
    }

    /// Subscribe to vfs key updates for a timeline.
    ///
    /// Uses Oxia sequence key subscription to receive the highest vfs key
    /// each time a new vfs is written. The receiver yields the full key
    /// string (e.g. `/chronicle/timelines/{name}/seg-0000000000000000001`).
    pub async fn subscribe_segments(
        &self,
        timeline_name: &str,
    ) -> Result<Receiver<String>, CatalogError> {
        let key = crate::segment_key_prefix(timeline_name);
        let partition_key = timeline_name.to_string();
        self.client
            .get_sequence_updates_with_options(
                key,
                vec![GetSequenceUpdatesOption::PartitionKey(partition_key)],
            )
            .await
            .map_err(CatalogError::from)
    }
}

#[async_trait]
impl Catalog for OxiaCatalog {
    async fn get_timeline(&self, name: &str) -> Result<TimelineMeta, CatalogError> {
        OxiaCatalog::get_timeline(self, name).await
    }

    async fn timeline_update(
        &self,
        meta: &TimelineMeta,
        expected_version: i64,
    ) -> Result<TimelineMeta, CatalogError> {
        OxiaCatalog::timeline_update(self, meta, expected_version).await
    }

    async fn create_timeline(&self, name: &str) -> Result<TimelineMeta, CatalogError> {
        OxiaCatalog::create_timeline(self, name).await
    }

    async fn delete_timeline(&self, name: &str, expected_version: i64) -> Result<(), CatalogError> {
        OxiaCatalog::delete_timeline(self, name, expected_version).await
    }

    async fn list_timelines(&self) -> Result<Vec<TimelineMeta>, CatalogError> {
        OxiaCatalog::list_timelines(self).await
    }

    async fn put_segment(
        &self,
        timeline_name: &str,
        segment: &Segment,
        expected_version: i64,
    ) -> Result<Versioned<Segment>, CatalogError> {
        OxiaCatalog::put_segment(self, timeline_name, segment, expected_version).await
    }

    async fn list_segments(
        &self,
        timeline_name: &str,
    ) -> Result<Vec<Versioned<Segment>>, CatalogError> {
        OxiaCatalog::list_segments(self, timeline_name).await
    }

    async fn get_last_segment(
        &self,
        timeline_name: &str,
    ) -> Result<Option<Versioned<Segment>>, CatalogError> {
        OxiaCatalog::get_last_segment(self, timeline_name).await
    }

    async fn get_segment_for_offset(
        &self,
        timeline_name: &str,
        offset: i64,
    ) -> Result<Option<Versioned<Segment>>, CatalogError> {
        OxiaCatalog::get_segment_for_offset(self, timeline_name, offset).await
    }

    async fn tl_fetch_or_insert(&self, name: &str) -> Result<TimelineMeta, CatalogError> {
        OxiaCatalog::tl_fetch_or_insert(self, name).await
    }

    async fn tl_new_term(&self, name: &str) -> Result<TimelineMeta, CatalogError> {
        OxiaCatalog::tl_new_term(self, name).await
    }

    async fn register_unit(&self, registration: &UnitRegistration) -> Result<(), CatalogError> {
        OxiaCatalog::register_unit(self, registration).await
    }

    async fn unregister_unit(&self, address: &str, zone: &str) -> Result<(), CatalogError> {
        OxiaCatalog::unregister_unit(self, address, zone).await
    }

    async fn list_units(&self) -> Result<Vec<UnitRegistration>, CatalogError> {
        OxiaCatalog::list_units(self).await
    }

    async fn list_writable_units(&self) -> Result<Vec<UnitRegistration>, CatalogError> {
        OxiaCatalog::list_writable_units(self).await
    }

    async fn subscribe_segments(
        &self,
        timeline_name: &str,
    ) -> Result<Receiver<String>, CatalogError> {
        OxiaCatalog::subscribe_segments(self, timeline_name).await
    }
}
