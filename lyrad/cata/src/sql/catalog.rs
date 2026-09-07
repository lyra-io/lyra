use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{MemorySchemaProvider, SchemaProvider, Session, TableProvider};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_pg_catalog::pg_catalog::context::{PgCatalogContextProvider, Role};
use meta::metadata::Metadata;
use std::fmt;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct CatalogContextProvider {
    // Immutable state
    metadata: Arc<dyn Metadata>,
}

impl CatalogContextProvider {
    pub(crate) fn new(metadata: Arc<dyn Metadata>) -> Self {
        Self { metadata }
    }
}

impl fmt::Debug for CatalogContextProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogContextProvider")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl PgCatalogContextProvider for CatalogContextProvider {
    async fn roles(&self) -> Vec<String> {
        let mut names = self
            .metadata
            .list_users()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|record| record.value().name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    async fn role(&self, name: &str) -> Option<Role> {
        let user = self.metadata.get_user(name).await.ok()??;
        Some(Role {
            name: user.value().name.clone(),
            is_superuser: false,
            can_login: user.value().password.is_some(),
            can_create_db: false,
            can_create_role: false,
            can_create_user: false,
            can_replication: false,
            grants: Vec::new(),
            inherited_roles: Vec::new(),
        })
    }
}

pub(crate) fn register_rw_catalog(
    context: &SessionContext,
    database: &str,
    metadata: Arc<dyn Metadata>,
) -> DataFusionResult<()> {
    let catalog = context.catalog(database).ok_or_else(|| {
        DataFusionError::Configuration(format!(
            "catalog not found when registering rw_catalog: {database}"
        ))
    })?;
    let schema = Arc::new(MemorySchemaProvider::new());
    schema.register_table(
        "rw_users".to_string(),
        Arc::new(RwUsersTable::new(metadata)),
    )?;
    catalog.register_schema("rw_catalog", schema)?;
    Ok(())
}

struct RwUsersTable {
    // Immutable state
    schema: SchemaRef,
    metadata: Arc<dyn Metadata>,
}

impl RwUsersTable {
    fn new(metadata: Arc<dyn Metadata>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("is_super", DataType::Boolean, false),
            Field::new("create_db", DataType::Boolean, false),
            Field::new("create_user", DataType::Boolean, false),
            Field::new("can_login", DataType::Boolean, false),
            Field::new("is_admin", DataType::Boolean, false),
        ]));
        Self { schema, metadata }
    }
}

impl fmt::Debug for RwUsersTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RwUsersTable")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for RwUsersTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut users = self
            .metadata
            .list_users()
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        users.sort_by(|left, right| left.value().name.cmp(&right.value().name));
        users.truncate(limit.unwrap_or(users.len()).min(users.len()));

        let names = users
            .iter()
            .map(|user| user.value().name.as_str())
            .collect::<Vec<_>>();
        let is_super = vec![false; users.len()];
        let create_db = vec![false; users.len()];
        let create_user = vec![false; users.len()];
        let can_login = users
            .iter()
            .map(|user| user.value().password.is_some())
            .collect::<Vec<_>>();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(names)),
            Arc::new(BooleanArray::from(is_super.clone())),
            Arc::new(BooleanArray::from(create_db)),
            Arc::new(BooleanArray::from(create_user)),
            Arc::new(BooleanArray::from(can_login)),
            Arc::new(BooleanArray::from(vec![false; users.len()])),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays)?;
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            Arc::clone(&self.schema),
            projection.cloned(),
        )?)
    }
}
