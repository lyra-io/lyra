use crate::Result;
use crate::error::to_pgwire_error;
use crate::sql::{CatalogStatement, CreateSecret, SecretStatement};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::sqlparser::ast::{DollarQuotedString, Statement, Value};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::Token;
use datafusion_postgres::Parser as DataFusionParser;
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::FieldInfo;
use datafusion_postgres::pgwire::api::stmt::QueryParser;
use datafusion_postgres::pgwire::api::{ClientInfo, Type};
use datafusion_postgres::pgwire::error::PgWireResult;
use std::sync::Arc;

pub(crate) type DataFusionStatement = (String, Option<(Statement, LogicalPlan)>);

pub fn parse_catalog_statement(sql: &str) -> Result<Option<CatalogStatement>> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(sql)?;

    if !parser.parse_keywords(&[Keyword::CREATE, Keyword::SECRET]) {
        return Ok(None);
    }

    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let identifier = parser.parse_identifier()?;
    let name = if identifier.quote_style.is_some() {
        identifier.value
    } else {
        identifier.value.to_ascii_lowercase()
    };

    parser.expect_keyword(Keyword::VALUE)?;
    let value = match parser.parse_value()?.value {
        Value::SingleQuotedString(value)
        | Value::EscapedStringLiteral(value)
        | Value::DollarQuotedString(DollarQuotedString { value, .. }) => value.into_bytes(),
        _ => {
            return Err(ParserError::ParserError(
                "CREATE SECRET value must be a string literal".to_string(),
            )
            .into());
        }
    };

    let _ = parser.consume_token(&Token::SemiColon);
    parser.expect_token(&Token::EOF)?;

    Ok(Some(CatalogStatement::Secret(SecretStatement::Create(
        CreateSecret::new(name, value, if_not_exists),
    ))))
}

pub(crate) struct CataQueryParser {
    // Immutable state
    datafusion: Arc<DataFusionParser>,
}

impl CataQueryParser {
    pub(crate) fn new(datafusion: Arc<DataFusionParser>) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_secret() {
        let statement =
            parse_catalog_statement("CREATE SECRET IF NOT EXISTS Kafka_Password VALUE 's3cr3t';")
                .unwrap()
                .unwrap();

        let CatalogStatement::Secret(SecretStatement::Create(statement)) = statement;
        assert_eq!(statement.name(), "kafka_password");
        assert_eq!(statement.value(), b"s3cr3t");
        assert!(statement.if_not_exists());
    }

    #[test]
    fn preserves_quoted_secret_name() {
        let statement =
            parse_catalog_statement("CREATE SECRET \"Kafka_Password\" VALUE $$multi\nline$$")
                .unwrap()
                .unwrap();

        let CatalogStatement::Secret(SecretStatement::Create(statement)) = statement;
        assert_eq!(statement.name(), "Kafka_Password");
        assert_eq!(statement.value(), b"multi\nline");
    }

    #[test]
    fn leaves_regular_sql_for_datafusion() {
        assert_eq!(parse_catalog_statement("SELECT 1").unwrap(), None);
    }

    #[test]
    fn rejects_non_string_secret_value() {
        let error = parse_catalog_statement("CREATE SECRET password VALUE 42").unwrap_err();

        assert!(error.to_string().contains("must be a string literal"));
    }

    #[test]
    fn rejects_as_before_secret_value() {
        let error = parse_catalog_statement("CREATE SECRET password AS 's3cr3t'").unwrap_err();

        assert!(error.to_string().contains("VALUE"), "{error}");
    }
}
