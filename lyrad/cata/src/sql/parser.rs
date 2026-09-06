use crate::Result;
use crate::error::to_pgwire_error;
use crate::handler::{DatabaseHandles, client_database};
use crate::sql::{
    AlterSecret, CatalogStatement, CreateSecret, DropSecret, SecretName, SecretStatement,
    ShowSecrets,
};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::sqlparser::ast::{DollarQuotedString, Statement, Value};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::Token;
use datafusion_postgres::Parser as DataFusionParser;
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::query::ExtendedQueryHandler;
use datafusion_postgres::pgwire::api::results::{FieldFormat, FieldInfo};
use datafusion_postgres::pgwire::api::stmt::QueryParser;
use datafusion_postgres::pgwire::api::{ClientInfo, Type};
use datafusion_postgres::pgwire::error::PgWireResult;
use std::sync::Arc;

pub(crate) type DataFusionStatement = (String, Option<(Statement, LogicalPlan)>);

pub fn parse_catalog_statement(sql: &str) -> Result<Option<CatalogStatement>> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect).try_with_sql(sql)?;

    let statement = if parser.parse_keywords(&[Keyword::CREATE, Keyword::SECRET]) {
        let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let secret = parse_secret_name0(&mut parser)?;
        parser.expect_keyword(Keyword::VALUE)?;
        let value = parse_secret_value0(&mut parser, "CREATE SECRET")?;
        SecretStatement::Create(CreateSecret::new(secret, value, if_not_exists))
    } else if parser.parse_keywords(&[Keyword::ALTER, Keyword::SECRET]) {
        let secret = parse_secret_name0(&mut parser)?;
        parser.expect_keyword(Keyword::VALUE)?;
        let value = parse_secret_value0(&mut parser, "ALTER SECRET")?;
        SecretStatement::Alter(AlterSecret::new(secret, value))
    } else if parser.parse_keywords(&[Keyword::DROP, Keyword::SECRET]) {
        let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        SecretStatement::Drop(DropSecret::new(parse_secret_name0(&mut parser)?, if_exists))
    } else if parser.parse_keyword(Keyword::SHOW) {
        let object = parse_identifier0(&mut parser)?;
        if !object.eq_ignore_ascii_case("secrets") {
            return Ok(None);
        }

        let schema = if parser.parse_keyword(Keyword::FROM) {
            Some(parse_identifier0(&mut parser)?)
        } else {
            None
        };
        let like = if parser.parse_keyword(Keyword::LIKE) {
            Some(parse_secret_pattern0(&mut parser)?)
        } else {
            None
        };
        SecretStatement::Show(ShowSecrets::new(schema, like))
    } else {
        return Ok(None);
    };

    let _ = parser.consume_token(&Token::SemiColon);
    parser.expect_token(&Token::EOF)?;
    Ok(Some(CatalogStatement::Secret(statement)))
}

fn parse_secret_value0(parser: &mut Parser, statement: &str) -> Result<Vec<u8>> {
    let value = match parser.parse_value()?.value {
        Value::SingleQuotedString(value)
        | Value::EscapedStringLiteral(value)
        | Value::DollarQuotedString(DollarQuotedString { value, .. }) => value.into_bytes(),
        _ => {
            return Err(ParserError::ParserError(format!(
                "{statement} value must be a string literal"
            ))
            .into());
        }
    };
    Ok(value)
}

fn parse_secret_pattern0(parser: &mut Parser) -> Result<String> {
    match parser.parse_value()?.value {
        Value::SingleQuotedString(value) | Value::EscapedStringLiteral(value) => Ok(value),
        _ => Err(ParserError::ParserError(
            "SHOW SECRETS LIKE pattern must be a string literal".to_string(),
        )
        .into()),
    }
}

fn parse_secret_name0(parser: &mut Parser) -> Result<SecretName> {
    let first = parse_identifier0(parser)?;
    if parser.consume_token(&Token::Period) {
        Ok(SecretName::new(Some(first), parse_identifier0(parser)?))
    } else {
        Ok(SecretName::new(None, first))
    }
}

fn parse_identifier0(parser: &mut Parser) -> Result<String> {
    let identifier = parser.parse_identifier()?;
    if identifier.quote_style.is_some() {
        Ok(identifier.value)
    } else {
        Ok(identifier.value.to_ascii_lowercase())
    }
}

pub(crate) fn show_secrets_fields(column_format: Option<&Format>) -> Vec<FieldInfo> {
    let format = column_format
        .map(|format| format.format_for(0))
        .unwrap_or(FieldFormat::Text);
    vec![FieldInfo::new(
        "Name".to_string(),
        None,
        None,
        Type::VARCHAR,
        format,
    )]
}

pub(crate) struct CataQueryParser {
    // Immutable state
    databases: Arc<DatabaseHandles>,
    default_parser: Arc<DataFusionParser>,
}

impl CataQueryParser {
    pub(crate) fn new(databases: Arc<DatabaseHandles>) -> Self {
        let default_parser = databases.default().query_parser();
        Self {
            databases,
            default_parser,
        }
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

        self.databases
            .get(client_database(client))
            .map_err(to_pgwire_error)?
            .query_parser()
            .parse_sql(client, sql, types)
            .await
    }

    fn get_parameter_types(&self, statement: &Self::Statement) -> PgWireResult<Vec<Type>> {
        self.default_parser.get_parameter_types(statement)
    }

    fn get_result_schema(
        &self,
        statement: &Self::Statement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        if let Some(statement) = parse_catalog_statement(&statement.0).map_err(to_pgwire_error)? {
            return if matches!(
                statement,
                CatalogStatement::Secret(SecretStatement::Show(_))
            ) {
                Ok(show_secrets_fields(column_format))
            } else {
                Ok(Vec::new())
            };
        }

        self.default_parser
            .get_result_schema(statement, column_format)
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

        let CatalogStatement::Secret(SecretStatement::Create(statement)) = statement else {
            panic!("expected CREATE SECRET")
        };
        assert_eq!(statement.secret().schema(), None);
        assert_eq!(statement.secret().name(), "kafka_password");
        assert_eq!(statement.value(), b"s3cr3t");
        assert!(statement.if_not_exists());
    }

    #[test]
    fn parses_qualified_quoted_secret_name() {
        let statement =
            parse_catalog_statement("CREATE SECRET Vault.\"Kafka_Password\" VALUE $$multi\nline$$")
                .unwrap()
                .unwrap();

        let CatalogStatement::Secret(SecretStatement::Create(statement)) = statement else {
            panic!("expected CREATE SECRET")
        };
        assert_eq!(statement.secret().schema(), Some("vault"));
        assert_eq!(statement.secret().name(), "Kafka_Password");
        assert_eq!(statement.value(), b"multi\nline");
    }

    #[test]
    fn parses_alter_secret() {
        let statement =
            parse_catalog_statement("ALTER SECRET public.kafka_password VALUE 'rotated'")
                .unwrap()
                .unwrap();

        let CatalogStatement::Secret(SecretStatement::Alter(statement)) = statement else {
            panic!("expected ALTER SECRET")
        };
        assert_eq!(statement.secret().schema(), Some("public"));
        assert_eq!(statement.secret().name(), "kafka_password");
        assert_eq!(statement.value(), b"rotated");
    }

    #[test]
    fn parses_drop_secret() {
        let statement = parse_catalog_statement("DROP SECRET IF EXISTS public.kafka_password")
            .unwrap()
            .unwrap();

        let CatalogStatement::Secret(SecretStatement::Drop(statement)) = statement else {
            panic!("expected DROP SECRET")
        };
        assert_eq!(statement.secret().schema(), Some("public"));
        assert_eq!(statement.secret().name(), "kafka_password");
        assert!(statement.if_exists());
    }

    #[test]
    fn parses_show_secrets_from_schema_like_pattern() {
        let statement = parse_catalog_statement("SHOW SECRETS FROM Private LIKE 'kafka%'")
            .unwrap()
            .unwrap();

        let CatalogStatement::Secret(SecretStatement::Show(statement)) = statement else {
            panic!("expected SHOW SECRETS")
        };
        assert_eq!(statement.schema(), Some("private"));
        assert_eq!(statement.like(), Some("kafka%"));
    }

    #[test]
    fn parses_show_secrets_without_filter() {
        let statement = parse_catalog_statement("SHOW SECRETS").unwrap().unwrap();

        let CatalogStatement::Secret(SecretStatement::Show(statement)) = statement else {
            panic!("expected SHOW SECRETS")
        };
        assert_eq!(statement.schema(), None);
        assert_eq!(statement.like(), None);
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
    fn rejects_non_string_alter_secret_value() {
        let error = parse_catalog_statement("ALTER SECRET password VALUE 42").unwrap_err();

        assert!(error.to_string().contains("must be a string literal"));
    }

    #[test]
    fn rejects_as_before_secret_value() {
        let error = parse_catalog_statement("CREATE SECRET password AS 's3cr3t'").unwrap_err();

        assert!(error.to_string().contains("VALUE"), "{error}");
    }
}
