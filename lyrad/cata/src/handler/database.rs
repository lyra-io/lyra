use crate::Result;
use crate::sql::SqlPlanner;
use dashmap::DashMap;
use datafusion_postgres::DfSessionService;
use meta::metadata::DEFAULT_DATABASE_NAME;
use std::sync::Arc;

pub(crate) struct DatabaseHandles {
    // Immutable state
    default: Arc<DfSessionService>,

    // Mutable state
    handles: DashMap<String, Arc<DfSessionService>>,
}

impl DatabaseHandles {
    pub(crate) fn new() -> Result<Self> {
        let default = Self::handle0(DEFAULT_DATABASE_NAME)?;
        let handles =
            DashMap::from_iter([(DEFAULT_DATABASE_NAME.to_string(), Arc::clone(&default))]);
        Ok(Self { default, handles })
    }

    pub(crate) fn get(&self, database: &str) -> Result<Arc<DfSessionService>> {
        let handle = self
            .handles
            .entry(database.to_string())
            .or_try_insert_with(|| Self::handle0(database))?;
        Ok(Arc::clone(handle.value()))
    }

    pub(crate) fn default(&self) -> &Arc<DfSessionService> {
        &self.default
    }

    pub(crate) fn remove(&self, database: &str) {
        self.handles.remove(database);
    }

    fn handle0(database: &str) -> Result<Arc<DfSessionService>> {
        let planner = SqlPlanner::new(database)?;
        Ok(Arc::new(DfSessionService::new(Arc::clone(
            planner.context(),
        ))))
    }
}
