mod handler;
mod parser;
mod server;
mod session;

pub use server::PostgresServer;

use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::sqlparser::ast::Statement;
use session::CataSessionService;

type DataFusionStatement = (String, Option<(Statement, LogicalPlan)>);
