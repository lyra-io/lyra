use crate::CataError;
use crate::postgres::DataFusionStatement;
use crate::query::parse_catalog_statement;
use async_trait::async_trait;
use datafusion_postgres::Parser as DataFusionParser;
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::FieldInfo;
use datafusion_postgres::pgwire::api::stmt::QueryParser;
use datafusion_postgres::pgwire::api::{ClientInfo, Type};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use std::sync::Arc;

pub struct CataQueryParser {
    // Immutable state
    datafusion: Arc<DataFusionParser>,
}

impl CataQueryParser {
    pub fn new(datafusion: Arc<DataFusionParser>) -> Self {
        Self { datafusion }
    }
}

#[async_trait]
impl QueryParser for CataQueryParser {
    type Statement = DataFusionStatement;

    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Self::Statement>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        if parse_catalog_statement(sql)
            .map_err(to_pgwire_error)?
            .is_some()
        {
            return Ok((sql.to_string(), None));
        }

        self.datafusion.parse_sql(client, sql, types).await
    }

    fn get_parameter_types(&self, statement: &Self::Statement) -> PgWireResult<Vec<Type>> {
        self.datafusion.get_parameter_types(statement)
    }

    fn get_result_schema(
        &self,
        statement: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        self.datafusion.get_result_schema(statement, column_format)
    }
}

pub(crate) fn to_pgwire_error(error: CataError) -> PgWireError {
    let code = match error {
        CataError::Sql(_) => "42601",
        CataError::SecretAlreadyExists(_) => "42710",
        _ => "XX000",
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.to_string(),
        error.to_string(),
    )))
}
