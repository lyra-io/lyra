use crate::handler::SecretHandler;
use crate::options::CataOptions;
use crate::sql::{CatalogStatement, SecretStatement, SqlPlanner, parse_catalog_statement};
use crate::{CataError, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion_postgres::DfSessionService;
use datafusion_postgres::Parser as DataFusionParser;
use datafusion_postgres::pgwire::api::PgWireServerHandlers;
use datafusion_postgres::pgwire::api::auth::StartupHandler;
use datafusion_postgres::pgwire::api::auth::noop::NoopStartupHandler;
use datafusion_postgres::pgwire::api::cancel::{CancelHandler, DefaultCancelHandler};
use datafusion_postgres::pgwire::api::portal::{Format, Portal};
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::results::{FieldInfo, Response, Tag};
use datafusion_postgres::pgwire::api::stmt::QueryParser;
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::{
    ClientInfo, ClientPortalStore, ConnectionManager, ErrorHandler, NoopHandler, Type,
};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use datafusion_postgres::{ServerOptions, serve_with_handlers};
use futures_util::Sink;
use meta::metadata::Metadata;
use std::fmt::Debug;
use std::sync::Arc;

type DataFusionStatement = (String, Option<(Statement, LogicalPlan)>);

pub struct Cata {
    // Control state
    cancel: Arc<DefaultCancelHandler>,

    // Immutable state
    metadata: Arc<dyn Metadata>,
    options: CataOptions,
    planner: SqlPlanner,
    session: Arc<SessionHandler>,
    startup: Arc<PostgresStartupHandler>,
}

impl Cata {
    pub fn new(options: CataOptions, metadata: Arc<dyn Metadata>) -> Result<Self> {
        let planner = SqlPlanner::new()?;
        let connection_manager = Arc::new(ConnectionManager::new());
        let cancel = Arc::new(DefaultCancelHandler::new(Arc::clone(&connection_manager)));
        let session = Arc::new(SessionHandler::new(
            Arc::clone(planner.context()),
            Arc::clone(&metadata),
        ));
        let startup = Arc::new(PostgresStartupHandler::new(connection_manager));
        Ok(Self {
            cancel,
            metadata,
            options,
            planner,
            session,
            startup,
        })
    }

    pub fn metadata(&self) -> &Arc<dyn Metadata> {
        &self.metadata
    }

    pub async fn plan(&self, sql: &str) -> Result<LogicalPlan> {
        self.planner.plan(sql).await
    }

    pub async fn serve(self) -> Result<()> {
        let postgres = self.options.postgres();
        let options = ServerOptions::new()
            .with_host(postgres.host().to_string())
            .with_port(postgres.port())
            .with_max_connections(postgres.max_connections());

        serve_with_handlers(Arc::new(self), &options).await?;
        Ok(())
    }
}

impl PgWireServerHandlers for Cata {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.session)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.session)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::clone(&self.startup)
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(NoopHandler)
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::clone(&self.cancel)
    }
}

struct PostgresStartupHandler {
    // Immutable state
    connection_manager: Arc<ConnectionManager>,
}

impl PostgresStartupHandler {
    fn new(connection_manager: Arc<ConnectionManager>) -> Self {
        Self { connection_manager }
    }
}

impl NoopStartupHandler for PostgresStartupHandler {
    fn connection_manager(&self) -> Option<Arc<ConnectionManager>> {
        Some(Arc::clone(&self.connection_manager))
    }
}

struct SessionHandler {
    // Immutable state
    datafusion: Arc<DfSessionService>,
    parser: Arc<CataQueryParser>,
    secrets: SecretHandler,
}

impl SessionHandler {
    fn new(context: Arc<SessionContext>, metadata: Arc<dyn Metadata>) -> Self {
        let datafusion = Arc::new(DfSessionService::new(context));
        let parser = Arc::new(CataQueryParser::new(datafusion.query_parser()));
        let secrets = SecretHandler::new(metadata);
        Self {
            datafusion,
            parser,
            secrets,
        }
    }

    async fn execute(&self, statement: CatalogStatement) -> PgWireResult<Response> {
        match statement {
            CatalogStatement::Secret(SecretStatement::Create(statement)) => {
                self.secrets
                    .create(&statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("CREATE SECRET")))
            }
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for SessionHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let Some(statement) = parse_catalog_statement(query).map_err(to_pgwire_error)? {
            return Ok(vec![self.execute(statement).await?]);
        }

        SimpleQueryHandler::do_query(self.datafusion.as_ref(), client, query).await
    }
}

#[async_trait]
impl ExtendedQueryHandler for SessionHandler {
    type Statement = DataFusionStatement;
    type QueryParser = CataQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.parser)
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = &portal.statement.statement.0;
        if let Some(statement) = parse_catalog_statement(sql).map_err(to_pgwire_error)? {
            return self.execute(statement).await;
        }

        ExtendedQueryHandler::do_query(self.datafusion.as_ref(), client, portal, max_rows).await
    }
}

struct CataQueryParser {
    // Immutable state
    datafusion: Arc<DataFusionParser>,
}

impl CataQueryParser {
    fn new(datafusion: Arc<DataFusionParser>) -> Self {
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

fn to_pgwire_error(error: CataError) -> PgWireError {
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
