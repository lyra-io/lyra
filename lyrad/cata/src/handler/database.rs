use crate::Result;
use crate::sql::SqlPlanner;
use dashmap::DashMap;
use datafusion_postgres::DfSessionService;
use meta::metadata::{DEFAULT_DATABASE_NAME, Metadata};
use std::sync::Arc;

pub(crate) struct DatabaseHandles {
    // Immutable state
    default: Arc<DfSessionService>,
    metadata: Arc<dyn Metadata>,

    // Mutable state
    handles: DashMap<String, Arc<DfSessionService>>,
}

impl DatabaseHandles {
    pub(crate) fn new(metadata: Arc<dyn Metadata>) -> Result<Self> {
        let default = Self::handle0(DEFAULT_DATABASE_NAME, Arc::clone(&metadata))?;
        let handles =
            DashMap::from_iter([(DEFAULT_DATABASE_NAME.to_string(), Arc::clone(&default))]);
        Ok(Self {
            default,
            metadata,
            handles,
        })
    }

    pub(crate) fn get(&self, database: &str) -> Result<Arc<DfSessionService>> {
        let handle = self
            .handles
            .entry(database.to_string())
            .or_try_insert_with(|| Self::handle0(database, Arc::clone(&self.metadata)))?;
        Ok(Arc::clone(handle.value()))
    }

    pub(crate) fn default(&self) -> &Arc<DfSessionService> {
        &self.default
    }

    pub(crate) fn remove(&self, database: &str) {
        self.handles.remove(database);
    }

    fn handle0(database: &str, metadata: Arc<dyn Metadata>) -> Result<Arc<DfSessionService>> {
        let planner = SqlPlanner::new(database, metadata)?;
        Ok(Arc::new(DfSessionService::new(Arc::clone(
            planner.context(),
        ))))
    }
}
