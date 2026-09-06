mod handler;
mod parser;
mod session;

pub(crate) use handler::ProtocolHandler;

use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::sqlparser::ast::Statement;
use session::SessionHandler;

type DataFusionStatement = (String, Option<(Statement, LogicalPlan)>);
