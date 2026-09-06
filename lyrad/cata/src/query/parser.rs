use crate::Result;
use crate::query::{CatalogStatement, CreateSecret};
use datafusion::sql::sqlparser::ast::{DollarQuotedString, Value};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use datafusion::sql::sqlparser::tokenizer::Token;

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

    parser.expect_keyword(Keyword::AS)?;
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

    Ok(Some(CatalogStatement::CreateSecret(CreateSecret::new(
        name,
        value,
        if_not_exists,
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_create_secret() {
        let statement =
            parse_catalog_statement("CREATE SECRET IF NOT EXISTS Kafka_Password AS 's3cr3t';")
                .unwrap()
                .unwrap();

        let CatalogStatement::CreateSecret(statement) = statement;
        assert_eq!(statement.name(), "kafka_password");
        assert_eq!(statement.value(), b"s3cr3t");
        assert!(statement.if_not_exists());
    }

    #[test]
    fn preserves_quoted_secret_name() {
        let statement =
            parse_catalog_statement("CREATE SECRET \"Kafka_Password\" AS $$multi\nline$$")
                .unwrap()
                .unwrap();

        let CatalogStatement::CreateSecret(statement) = statement;
        assert_eq!(statement.name(), "Kafka_Password");
        assert_eq!(statement.value(), b"multi\nline");
    }

    #[test]
    fn leaves_regular_sql_for_datafusion() {
        assert_eq!(parse_catalog_statement("SELECT 1").unwrap(), None);
    }

    #[test]
    fn rejects_non_string_secret_value() {
        let error = parse_catalog_statement("CREATE SECRET password AS 42").unwrap_err();

        assert!(error.to_string().contains("must be a string literal"));
    }
}
