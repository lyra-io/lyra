pub mod error;
pub mod flight_sql;

use catalog::{
    Action, ActionKind, ActionRequest, CatalogRef, DataType, Dataset, DatasetField, DatasetSchema,
    Versioned,
};
use datafusion_sql::parser::{DFParser, Statement as DataFusionStatement};
use datafusion_sql::sqlparser::ast::{
    Expr, GroupByExpr, LimitClause, Query, Select, SelectItem, SetExpr, Statement as SqlStatement,
    TableFactor,
};
use error::LensError;
use libxunit::{RowBatch, ScanRequest, XunitClient};
use std::sync::Arc;

pub struct Lens {
    catalog: CatalogRef,
    xunit: Option<Arc<dyn XunitClient>>,
}

const SELECT_USAGE: &str = "expected SELECT * FROM <dataset> [LIMIT n]";

impl Lens {
    pub fn new(catalog: CatalogRef) -> Self {
        Self {
            catalog,
            xunit: None,
        }
    }

    pub fn with_xunit(catalog: CatalogRef, xunit: Arc<dyn XunitClient>) -> Self {
        Self {
            catalog,
            xunit: Some(xunit),
        }
    }

    pub async fn execute(&self, sql: &str) -> Result<LensOutput, LensError> {
        let statement = sql.trim().trim_end_matches(';').trim();
        if statement.eq_ignore_ascii_case("show datasets") {
            return Ok(LensOutput::Datasets(self.catalog.list_datasets().await?));
        }
        if statement.eq_ignore_ascii_case("show databases")
            || statement.eq_ignore_ascii_case("show tables")
        {
            return Err(LensError::UnsupportedStatement(
                "Chronicle exposes datasets; use SHOW DATASETS".to_string(),
            ));
        }

        if let Some(dataset) = parse_create_dataset(statement)? {
            return Ok(LensOutput::Datasets(vec![
                self.catalog.create_dataset(dataset).await?,
            ]));
        }

        if let Some(request) = parse_action(statement)? {
            return Ok(LensOutput::Action(
                self.catalog.submit_action(request).await?,
            ));
        }

        if let Some(request) = parse_select(statement)? {
            let xunit = self.xunit.as_ref().ok_or(LensError::MissingXunitClient)?;
            return Ok(LensOutput::Rows(xunit.scan(request).await?.batches));
        }

        Err(LensError::UnsupportedStatement(statement.to_string()))
    }
}

#[derive(Debug, Clone)]
pub enum LensOutput {
    Empty,
    Message(String),
    Datasets(Vec<Versioned<Dataset>>),
    Action(Versioned<Action>),
    Rows(Vec<RowBatch>),
}

fn parse_select(statement: &str) -> Result<Option<ScanRequest>, LensError> {
    if !first_keyword_is(statement, "select") {
        return Ok(None);
    }

    let mut statements = DFParser::parse_sql(statement)
        .map_err(|_| LensError::InvalidStatement(SELECT_USAGE.to_string()))?;
    if statements.len() != 1 {
        return Err(LensError::InvalidStatement(SELECT_USAGE.to_string()));
    }

    let Some(DataFusionStatement::Statement(sql_statement)) = statements.pop_front() else {
        return Err(LensError::InvalidStatement(SELECT_USAGE.to_string()));
    };
    let SqlStatement::Query(query) = *sql_statement else {
        return Err(LensError::InvalidStatement(SELECT_USAGE.to_string()));
    };

    Ok(Some(scan_from_query(&query)?))
}

fn first_keyword_is(statement: &str, expected: &str) -> bool {
    statement
        .split_whitespace()
        .next()
        .map(|keyword| keyword.eq_ignore_ascii_case(expected))
        .unwrap_or(false)
}

fn scan_from_query(query: &Query) -> Result<ScanRequest, LensError> {
    if query.with.is_some()
        || query.order_by.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(LensError::InvalidStatement(SELECT_USAGE.to_string()));
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(LensError::InvalidStatement(SELECT_USAGE.to_string()));
    };

    let dataset = dataset_from_select(select)?;
    let mut request = ScanRequest::all(dataset);
    if let Some(limit) = limit_from_query(query)? {
        request = request.with_limit(limit);
    }
    Ok(request)
}

fn dataset_from_select(select: &Select) -> Result<String, LensError> {
    if select.distinct.is_some()
        || select.select_modifiers.is_some()
        || select.top.is_some()
        || select.projection.len() != 1
        || !matches!(select.projection[0], SelectItem::Wildcard(_))
        || select.from.len() != 1
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || select.selection.is_some()
        || !group_by_is_empty(&select.group_by)
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return Err(LensError::InvalidStatement(SELECT_USAGE.to_string()));
    }

    let table = &select.from[0];
    if !table.joins.is_empty() {
        return Err(LensError::InvalidStatement(SELECT_USAGE.to_string()));
    }

    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = &table.relation
    else {
        return Err(LensError::InvalidStatement(SELECT_USAGE.to_string()));
    };

    if alias.is_some()
        || args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return Err(LensError::InvalidStatement(SELECT_USAGE.to_string()));
    }

    Ok(name.to_string())
}

fn group_by_is_empty(group_by: &GroupByExpr) -> bool {
    match group_by {
        GroupByExpr::Expressions(expressions, modifiers) => {
            expressions.is_empty() && modifiers.is_empty()
        }
        GroupByExpr::All(_) => false,
    }
}

fn limit_from_query(query: &Query) -> Result<Option<usize>, LensError> {
    let Some(limit_clause) = &query.limit_clause else {
        return Ok(None);
    };

    match limit_clause {
        LimitClause::LimitOffset {
            limit: Some(limit),
            offset: None,
            limit_by,
        } if limit_by.is_empty() => parse_limit(limit).map(Some),
        _ => Err(LensError::InvalidStatement(SELECT_USAGE.to_string())),
    }
}

fn parse_limit(limit: &Expr) -> Result<usize, LensError> {
    match limit {
        Expr::Value(value) => value
            .to_string()
            .parse::<usize>()
            .map_err(|_| LensError::InvalidStatement(SELECT_USAGE.to_string())),
        _ => Err(LensError::InvalidStatement(SELECT_USAGE.to_string())),
    }
}

fn parse_create_dataset(statement: &str) -> Result<Option<Dataset>, LensError> {
    let lower = statement.to_ascii_lowercase();
    if !lower.starts_with("create dataset ") {
        return Ok(None);
    }

    let rest = statement["create dataset ".len()..].trim();
    let open = rest
        .find('(')
        .ok_or_else(|| LensError::InvalidStatement(statement.to_string()))?;
    let close = rest
        .rfind(')')
        .ok_or_else(|| LensError::InvalidStatement(statement.to_string()))?;
    if close <= open {
        return Err(LensError::InvalidStatement(statement.to_string()));
    }

    let name = rest[..open].trim();
    if name.is_empty() {
        return Err(LensError::InvalidStatement(statement.to_string()));
    }

    let fields_sql = &rest[open + 1..close];
    let mut fields = Vec::new();
    for field_sql in fields_sql.split(',') {
        let tokens: Vec<_> = field_sql.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(LensError::InvalidStatement(statement.to_string()));
        }
        let mut field = DatasetField::new(tokens[0], parse_data_type(tokens[1])?);
        if tokens.len() >= 4
            && tokens[2].eq_ignore_ascii_case("not")
            && tokens[3].eq_ignore_ascii_case("null")
        {
            field.nullable = false;
        }
        fields.push(field);
    }

    if fields.is_empty() {
        return Err(LensError::InvalidStatement(statement.to_string()));
    }

    Ok(Some(Dataset::new(name, DatasetSchema::new(fields))))
}

fn parse_action(statement: &str) -> Result<Option<ActionRequest>, LensError> {
    let tokens: Vec<_> = statement.split_whitespace().collect();
    if tokens.len() != 3 || !tokens[1].eq_ignore_ascii_case("dataset") {
        return Ok(None);
    }

    let kind = match tokens[0].to_ascii_lowercase().as_str() {
        "unload" => ActionKind::Unload,
        "offload" => ActionKind::Offload,
        "optimize" => ActionKind::Optimize,
        "compact" => ActionKind::Compact,
        "vacuum" => ActionKind::Vacuum,
        "refresh" => ActionKind::Refresh,
        _ => return Ok(None),
    };

    if tokens[2].is_empty() {
        return Err(LensError::InvalidStatement(statement.to_string()));
    }

    Ok(Some(ActionRequest::new(kind, tokens[2])))
}

fn parse_data_type(value: &str) -> Result<DataType, LensError> {
    match value.to_ascii_lowercase().as_str() {
        "bool" | "boolean" => Ok(DataType::Boolean),
        "int" | "int32" | "integer" => Ok(DataType::Int32),
        "bigint" | "int64" | "long" => Ok(DataType::Int64),
        "float" | "float32" => Ok(DataType::Float32),
        "double" | "float64" => Ok(DataType::Float64),
        "string" | "text" => Ok(DataType::String),
        "binary" | "bytes" => Ok(DataType::Binary),
        "date" => Ok(DataType::Date),
        "timestamp" => Ok(DataType::Timestamp),
        "json" => Ok(DataType::Json),
        _ => Err(LensError::InvalidStatement(format!(
            "unsupported data type: {value}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::{DataType, DatasetField, DatasetSchema, build_memory_catalog};
    use chronicle_xunit::Xunit;
    use libxunit::{AppendRowsRequest, RowData};

    #[tokio::test]
    async fn select_rows_from_xunit() {
        let catalog = build_memory_catalog();
        catalog
            .create_dataset(Dataset::new(
                "events",
                DatasetSchema::new(vec![DatasetField::new("payload", DataType::Json)]),
            ))
            .await
            .unwrap();

        let xunit = Arc::new(Xunit::new(catalog.clone()));
        xunit
            .append_rows(AppendRowsRequest::new(
                "events",
                "default",
                1,
                catalog::OffsetRange::new(0, 2),
                vec![RowData::new(0, b"one"), RowData::new(1, b"two")],
            ))
            .await
            .unwrap();

        let lens = Lens::with_xunit(catalog, xunit);
        let output = lens.execute("select * from events limit 1").await.unwrap();
        match output {
            LensOutput::Rows(batches) => {
                assert_eq!(batches.len(), 1);
                assert_eq!(batches[0].rows.len(), 1);
                assert_eq!(batches[0].rows[0].payload, b"one");
            }
            other => panic!("unexpected output: {other:?}"),
        }
    }

    #[tokio::test]
    async fn create_dataset_uses_catalog_sql() {
        let catalog = build_memory_catalog();
        let lens = Lens::new(catalog);

        let output = lens
            .execute("create dataset events (id int64 not null, payload json)")
            .await
            .unwrap();

        match output {
            LensOutput::Datasets(datasets) => {
                assert_eq!(datasets.len(), 1);
                assert_eq!(datasets[0].value.name, "events");
                assert_eq!(datasets[0].value.schema.fields.len(), 2);
                assert!(!datasets[0].value.schema.fields[0].nullable);
            }
            other => panic!("unexpected output: {other:?}"),
        }
    }

    #[tokio::test]
    async fn action_sql_submits_catalog_action() {
        let catalog = build_memory_catalog();
        catalog
            .create_dataset(Dataset::new(
                "events",
                DatasetSchema::new(vec![DatasetField::new("payload", DataType::Json)]),
            ))
            .await
            .unwrap();
        let lens = Lens::new(catalog);

        let output = lens.execute("unload dataset events").await.unwrap();

        match output {
            LensOutput::Action(action) => {
                assert_eq!(action.value.request.dataset, "events");
                assert_eq!(action.value.request.kind, ActionKind::Unload);
            }
            other => panic!("unexpected output: {other:?}"),
        }
    }

    #[tokio::test]
    async fn invalid_select_reports_supported_shape() {
        let catalog = build_memory_catalog();
        let lens = Lens::new(catalog);

        let error = lens.execute("select *;").await.unwrap_err();

        assert_eq!(
            error.to_string(),
            "Invalid SQL statement: expected SELECT * FROM <dataset> [LIMIT n]"
        );
    }

    #[tokio::test]
    async fn show_databases_points_to_datasets() {
        let catalog = build_memory_catalog();
        let lens = Lens::new(catalog);

        let error = lens.execute("show databases").await.unwrap_err();

        assert_eq!(
            error.to_string(),
            "Unsupported SQL statement: Chronicle exposes datasets; use SHOW DATASETS"
        );
    }

    #[tokio::test]
    async fn parsed_select_with_filter_is_rejected_until_planned() {
        let catalog = build_memory_catalog();
        let lens = Lens::new(catalog);

        let error = lens
            .execute("select * from events where id = 1")
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Invalid SQL statement: expected SELECT * FROM <dataset> [LIMIT n]"
        );
    }
}
