use async_trait::async_trait;
use lyra_proto::pb_catalog::{Segment, StreamMeta, UnitRegistration};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc::Receiver;

use crate::error::CatalogError;
use crate::{
    Action, ActionId, ActionRequest, ActionStatus, Catalog, CatalogRef, Dataset, DatasetName,
    Versioned,
};

#[derive(Default)]
pub struct MemoryCatalog {
    state: RwLock<MemoryCatalogState>,
}

#[derive(Default)]
struct MemoryCatalogState {
    datasets: HashMap<DatasetName, Versioned<Dataset>>,
    actions: HashMap<ActionId, Versioned<Action>>,
    next_version: i64,
    next_action_id: i64,
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    fn next_version(state: &mut MemoryCatalogState) -> i64 {
        state.next_version += 1;
        state.next_version
    }

    fn next_action_id(state: &mut MemoryCatalogState) -> ActionId {
        state.next_action_id += 1;
        format!("action-{}", state.next_action_id)
    }
}

pub fn build_memory_catalog() -> CatalogRef {
    Arc::new(MemoryCatalog::new())
}

#[async_trait]
impl Catalog for MemoryCatalog {
    async fn create_dataset(
        &self,
        mut dataset: Dataset,
    ) -> Result<Versioned<Dataset>, CatalogError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| CatalogError::Internal("memory catalog lock poisoned".into()))?;
        if state.datasets.contains_key(&dataset.name) {
            return Err(CatalogError::AlreadyExists(dataset.name));
        }
        let version = Self::next_version(&mut state);
        dataset.version = version;
        let versioned = Versioned::new(dataset.clone(), version);
        state
            .datasets
            .insert(dataset.name.clone(), versioned.clone());
        Ok(versioned)
    }

    async fn update_dataset(
        &self,
        mut dataset: Dataset,
        expected_version: i64,
    ) -> Result<Versioned<Dataset>, CatalogError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| CatalogError::Internal("memory catalog lock poisoned".into()))?;
        let current = state
            .datasets
            .get(&dataset.name)
            .ok_or_else(|| CatalogError::NotFound(dataset.name.clone()))?;
        if current.version != expected_version {
            return Err(CatalogError::VersionConflict {
                expected: expected_version,
                actual: current.version,
            });
        }
        let version = Self::next_version(&mut state);
        dataset.version = version;
        let versioned = Versioned::new(dataset.clone(), version);
        state
            .datasets
            .insert(dataset.name.clone(), versioned.clone());
        Ok(versioned)
    }

    async fn get_dataset(&self, name: &str) -> Result<Versioned<Dataset>, CatalogError> {
        let state = self
            .state
            .read()
            .map_err(|_| CatalogError::Internal("memory catalog lock poisoned".into()))?;
        state
            .datasets
            .get(name)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))
    }

    async fn list_datasets(&self) -> Result<Vec<Versioned<Dataset>>, CatalogError> {
        let state = self
            .state
            .read()
            .map_err(|_| CatalogError::Internal("memory catalog lock poisoned".into()))?;
        let mut datasets: Vec<_> = state.datasets.values().cloned().collect();
        datasets.sort_by(|left, right| left.value.name.cmp(&right.value.name));
        Ok(datasets)
    }

    async fn delete_dataset(&self, name: &str, expected_version: i64) -> Result<(), CatalogError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| CatalogError::Internal("memory catalog lock poisoned".into()))?;
        let current = state
            .datasets
            .get(name)
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;
        if current.version != expected_version {
            return Err(CatalogError::VersionConflict {
                expected: expected_version,
                actual: current.version,
            });
        }
        state.datasets.remove(name);
        Ok(())
    }

    async fn submit_action(
        &self,
        request: ActionRequest,
    ) -> Result<Versioned<Action>, CatalogError> {
        let mut state = self
            .state
            .write()
            .map_err(|_| CatalogError::Internal("memory catalog lock poisoned".into()))?;
        let id = Self::next_action_id(&mut state);
        let version = Self::next_version(&mut state);
        let action = Action {
            id: id.clone(),
            request,
            status: ActionStatus::Pending,
            message: None,
            version,
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        let versioned = Versioned::new(action, version);
        state.actions.insert(id, versioned.clone());
        Ok(versioned)
    }

    async fn get_action(&self, id: &ActionId) -> Result<Versioned<Action>, CatalogError> {
        let state = self
            .state
            .read()
            .map_err(|_| CatalogError::Internal("memory catalog lock poisoned".into()))?;
        state
            .actions
            .get(id)
            .cloned()
            .ok_or_else(|| CatalogError::NotFound(id.clone()))
    }

    async fn list_actions(
        &self,
        dataset: Option<&DatasetName>,
    ) -> Result<Vec<Versioned<Action>>, CatalogError> {
        let state = self
            .state
            .read()
            .map_err(|_| CatalogError::Internal("memory catalog lock poisoned".into()))?;
        let mut actions: Vec<_> = state
            .actions
            .values()
            .filter(|action| {
                dataset
                    .map(|dataset| action.value.request.dataset == *dataset)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        actions.sort_by(|left, right| left.value.id.cmp(&right.value.id));
        Ok(actions)
    }

    async fn get_stream(&self, name: &str) -> Result<StreamMeta, CatalogError> {
        Err(CatalogError::Unsupported(format!(
            "memory catalog stream get: {}",
            name
        )))
    }

    async fn stream_update(
        &self,
        _meta: &StreamMeta,
        _expected_version: i64,
    ) -> Result<StreamMeta, CatalogError> {
        Err(CatalogError::Unsupported(
            "memory catalog stream_update".into(),
        ))
    }

    async fn create_stream(&self, name: &str) -> Result<StreamMeta, CatalogError> {
        Err(CatalogError::Unsupported(format!(
            "memory catalog create_stream: {}",
            name
        )))
    }

    async fn delete_stream(&self, name: &str, _expected_version: i64) -> Result<(), CatalogError> {
        Err(CatalogError::Unsupported(format!(
            "memory catalog delete_stream: {}",
            name
        )))
    }

    async fn list_streams(&self) -> Result<Vec<StreamMeta>, CatalogError> {
        Ok(Vec::new())
    }

    async fn put_segment(
        &self,
        _stream_name: &str,
        _segment: &Segment,
        _expected_version: i64,
    ) -> Result<Versioned<Segment>, CatalogError> {
        Err(CatalogError::Unsupported(
            "memory catalog put_segment".into(),
        ))
    }

    async fn list_segments(
        &self,
        _stream_name: &str,
    ) -> Result<Vec<Versioned<Segment>>, CatalogError> {
        Ok(Vec::new())
    }

    async fn get_last_segment(
        &self,
        _stream_name: &str,
    ) -> Result<Option<Versioned<Segment>>, CatalogError> {
        Ok(None)
    }

    async fn get_segment_for_offset(
        &self,
        _stream_name: &str,
        _offset: i64,
    ) -> Result<Option<Versioned<Segment>>, CatalogError> {
        Ok(None)
    }

    async fn stream_get_or_insert(&self, name: &str) -> Result<StreamMeta, CatalogError> {
        self.create_stream(name).await
    }

    async fn stream_new_term(&self, name: &str) -> Result<StreamMeta, CatalogError> {
        self.create_stream(name).await
    }

    async fn register_unit(&self, _registration: &UnitRegistration) -> Result<(), CatalogError> {
        Ok(())
    }

    async fn unregister_unit(&self, _address: &str, _zone: &str) -> Result<(), CatalogError> {
        Ok(())
    }

    async fn list_units(&self) -> Result<Vec<UnitRegistration>, CatalogError> {
        Ok(Vec::new())
    }

    async fn list_writable_units(&self) -> Result<Vec<UnitRegistration>, CatalogError> {
        Ok(Vec::new())
    }

    async fn subscribe_segments(
        &self,
        _stream_name: &str,
    ) -> Result<Receiver<String>, CatalogError> {
        Err(CatalogError::Unsupported(
            "memory catalog subscribe_segments".into(),
        ))
    }
}
