use crate::Result;
use crate::sql::SqlPlanner;
use dashmap::DashMap;
use datafusion_postgres::DfSessionService;
use datafusion_postgres::pgwire::api::{ClientInfo, METADATA_DATABASE};
use meta::metadata::DEFAULT_DATABASE_NAME;
use meta::metadata::DEFAULT_SCHEMA_NAME;
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

pub(crate) fn client_database<C>(client: &C) -> &str
where
    C: ClientInfo,
{
    client
        .metadata()
        .get(METADATA_DATABASE)
        .map(String::as_str)
        .unwrap_or(DEFAULT_DATABASE_NAME)
}

pub(crate) fn client_schemas<C>(client: &C) -> Vec<String>
where
    C: ClientInfo,
{
    let schemas = client
        .metadata()
        .get("search_path")
        .into_iter()
        .flat_map(|search_path| search_path.split(','))
        .map(str::trim)
        .map(|schema| schema.trim_matches('"'))
        .filter(|schema| !schema.is_empty() && *schema != "$user")
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    if schemas.is_empty() {
        vec![DEFAULT_SCHEMA_NAME.to_string()]
    } else {
        schemas
    }
}
