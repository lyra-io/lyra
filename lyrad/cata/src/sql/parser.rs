use crate::Result;
use crate::error::to_pgwire_error;
use crate::handler::{DatabaseHandles, client_database};
use crate::sql::{
    AlterDatabase, AlterSchema, AlterSecret, AlterUser, AlterUserAction, CatalogStatement,
    CreateDatabase, CreateSchema, CreateSecret, CreateUser, DatabaseStatement, DropDatabase,
    DropSchema, DropSecret, DropUser, SchemaName, SchemaStatement, SecretName, SecretStatement,
    ShowDatabases, ShowSchemas, ShowSecrets, ShowUsers, UserOptions, UserPassword, UserStatement,
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
        CatalogStatement::Secret(SecretStatement::Create(CreateSecret::new(
            secret,
            value,
            if_not_exists,
        )))
    } else if parser.parse_keywords(&[Keyword::CREATE, Keyword::DATABASE]) {
        let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        CatalogStatement::Database(DatabaseStatement::Create(CreateDatabase::new(
            parse_identifier0(&mut parser)?,
            if_not_exists,
        )))
    } else if parser.parse_keywords(&[Keyword::CREATE, Keyword::SCHEMA]) {
        let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        CatalogStatement::Schema(SchemaStatement::Create(CreateSchema::new(
            parse_schema_name0(&mut parser)?,
            if_not_exists,
        )))
    } else if parser.parse_keywords(&[Keyword::CREATE, Keyword::USER]) {
        CatalogStatement::User(UserStatement::Create(CreateUser::new(
            parse_identifier0(&mut parser)?,
            parse_user_options0(&mut parser, false)?,
        )))
    } else if parser.parse_keywords(&[Keyword::ALTER, Keyword::SECRET]) {
        let secret = parse_secret_name0(&mut parser)?;
        parser.expect_keyword(Keyword::VALUE)?;
        let value = parse_secret_value0(&mut parser, "ALTER SECRET")?;
        CatalogStatement::Secret(SecretStatement::Alter(AlterSecret::new(secret, value)))
    } else if parser.parse_keywords(&[Keyword::ALTER, Keyword::DATABASE]) {
        let name = parse_identifier0(&mut parser)?;
        parser.expect_keywords(&[Keyword::RENAME, Keyword::TO])?;
        CatalogStatement::Database(DatabaseStatement::Alter(AlterDatabase::new(
            name,
            parse_identifier0(&mut parser)?,
        )))
    } else if parser.parse_keywords(&[Keyword::ALTER, Keyword::SCHEMA]) {
        let schema = parse_schema_name0(&mut parser)?;
        parser.expect_keywords(&[Keyword::RENAME, Keyword::TO])?;
        CatalogStatement::Schema(SchemaStatement::Alter(AlterSchema::new(
            schema,
            parse_identifier0(&mut parser)?,
        )))
    } else if parser.parse_keywords(&[Keyword::ALTER, Keyword::USER]) {
        let name = parse_identifier0(&mut parser)?;
        let action = if parser.parse_keywords(&[Keyword::RENAME, Keyword::TO]) {
            AlterUserAction::Rename(parse_identifier0(&mut parser)?)
        } else {
            AlterUserAction::Options(parse_user_options0(&mut parser, true)?)
        };
        CatalogStatement::User(UserStatement::Alter(AlterUser::new(name, action)))
    } else if parser.parse_keywords(&[Keyword::DROP, Keyword::SECRET]) {
        let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        CatalogStatement::Secret(SecretStatement::Drop(DropSecret::new(
            parse_secret_name0(&mut parser)?,
            if_exists,
        )))
    } else if parser.parse_keywords(&[Keyword::DROP, Keyword::DATABASE]) {
        let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        CatalogStatement::Database(DatabaseStatement::Drop(DropDatabase::new(
            parse_identifier0(&mut parser)?,
            if_exists,
        )))
    } else if parser.parse_keywords(&[Keyword::DROP, Keyword::SCHEMA]) {
        let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let schema = parse_schema_name0(&mut parser)?;
        let cascade = parser.parse_keyword(Keyword::CASCADE);
        CatalogStatement::Schema(SchemaStatement::Drop(DropSchema::new(
            schema, if_exists, cascade,
        )))
    } else if parser.parse_keywords(&[Keyword::DROP, Keyword::USER]) {
        let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
        let mut names = vec![parse_identifier0(&mut parser)?];
        while parser.consume_token(&Token::Comma) {
            names.push(parse_identifier0(&mut parser)?);
        }
        CatalogStatement::User(UserStatement::Drop(DropUser::new(names, if_exists)))
    } else if parser.parse_keyword(Keyword::SHOW) {
        let object = parse_identifier0(&mut parser)?;
        match object.as_str() {
            "databases" => CatalogStatement::Database(DatabaseStatement::Show(ShowDatabases::new(
                parse_like0(&mut parser, "SHOW DATABASES")?,
            ))),
            "schemas" => CatalogStatement::Schema(SchemaStatement::Show(ShowSchemas::new(
                parse_like0(&mut parser, "SHOW SCHEMAS")?,
            ))),
            "users" => CatalogStatement::User(UserStatement::Show(ShowUsers::new(parse_like0(
                &mut parser,
                "SHOW USERS",
            )?))),
            "secrets" => {
                let schema = if parser.parse_keyword(Keyword::FROM) {
                    Some(parse_identifier0(&mut parser)?)
                } else {
                    None
                };
                CatalogStatement::Secret(SecretStatement::Show(ShowSecrets::new(
                    schema,
                    parse_like0(&mut parser, "SHOW SECRETS")?,
                )))
            }
            _ => return Ok(None),
        }
    } else {
        return Ok(None);
    };

    let _ = parser.consume_token(&Token::SemiColon);
    parser.expect_token(&Token::EOF)?;
    Ok(Some(statement))
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

fn parse_like0(parser: &mut Parser, statement: &str) -> Result<Option<String>> {
    if !parser.parse_keyword(Keyword::LIKE) {
        return Ok(None);
    }

    match parser.parse_value()?.value {
        Value::SingleQuotedString(value) | Value::EscapedStringLiteral(value) => Ok(Some(value)),
        _ => Err(ParserError::ParserError(format!(
            "{statement} LIKE pattern must be a string literal"
        ))
        .into()),
    }
}

fn parse_user_options0(parser: &mut Parser, required: bool) -> Result<UserOptions> {
    let with = parser.parse_keyword(Keyword::WITH);
    let mut superuser = None;
    let mut create_database = None;
    let mut create_user = None;
    let mut password = UserPassword::Unchanged;
    let mut found = false;

    loop {
        if parser.parse_keyword(Keyword::SUPERUSER) {
            set_user_option0(&mut superuser, true, "SUPERUSER")?;
        } else if parser.parse_keyword(Keyword::NOSUPERUSER) {
            set_user_option0(&mut superuser, false, "SUPERUSER")?;
        } else if parser.parse_keyword(Keyword::CREATEDB) {
            set_user_option0(&mut create_database, true, "CREATEDB")?;
        } else if parser.parse_keyword(Keyword::NOCREATEDB) {
            set_user_option0(&mut create_database, false, "CREATEDB")?;
        } else if parse_word0(parser, "CREATEUSER") {
            set_user_option0(&mut create_user, true, "CREATEUSER")?;
        } else if parse_word0(parser, "NOCREATEUSER") {
            set_user_option0(&mut create_user, false, "CREATEUSER")?;
        } else if parser.parse_keyword(Keyword::PASSWORD) {
            if !matches!(password, UserPassword::Unchanged) {
                return Err(ParserError::ParserError(
                    "PASSWORD was specified more than once".to_string(),
                )
                .into());
            }
            password = parse_user_password0(parser)?;
        } else {
            break;
        }
        found = true;
    }

    if with && !found {
        return Err(
            ParserError::ParserError("WITH requires at least one user option".to_string()).into(),
        );
    }
    if required && !found {
        return Err(ParserError::ParserError(
            "ALTER USER requires RENAME TO or at least one user option".to_string(),
        )
        .into());
    }
    Ok(UserOptions::new(
        superuser,
        create_database,
        create_user,
        password,
    ))
}

fn parse_user_password0(parser: &mut Parser) -> Result<UserPassword> {
    if parser.parse_keyword(Keyword::NULL) {
        return Ok(UserPassword::Null);
    }
    match parser.parse_value()?.value {
        Value::SingleQuotedString(value)
        | Value::EscapedStringLiteral(value)
        | Value::DollarQuotedString(DollarQuotedString { value, .. }) => {
            Ok(UserPassword::Value(value))
        }
        _ => Err(
            ParserError::ParserError("PASSWORD must be a string literal or NULL".to_string())
                .into(),
        ),
    }
}

fn set_user_option0(option: &mut Option<bool>, value: bool, name: &str) -> Result<()> {
    if option.replace(value).is_some() {
        return Err(
            ParserError::ParserError(format!("{name} was specified more than once")).into(),
        );
    }
    Ok(())
}

fn parse_word0(parser: &mut Parser, expected: &str) -> bool {
    let matched = match parser.peek_token().token {
        Token::Word(word) => word.value.eq_ignore_ascii_case(expected),
        _ => false,
    };
    if matched {
        let _ = parser.next_token();
    }
    matched
}

fn parse_schema_name0(parser: &mut Parser) -> Result<SchemaName> {
    let first = parse_identifier0(parser)?;
    if parser.consume_token(&Token::Period) {
        Ok(SchemaName::new(Some(first), parse_identifier0(parser)?))
    } else {
        Ok(SchemaName::new(None, first))
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

pub(crate) fn show_names_fields(column_format: Option<&Format>) -> Vec<FieldInfo> {
    vec![FieldInfo::new(
        "Name".to_string(),
        None,
        None,
        Type::VARCHAR,
        format0(column_format, 0),
    )]
}

pub(crate) fn show_users_fields(column_format: Option<&Format>) -> Vec<FieldInfo> {
    vec![
        FieldInfo::new(
            "Name".to_string(),
            None,
            None,
            Type::VARCHAR,
            format0(column_format, 0),
        ),
        FieldInfo::new(
            "Superuser".to_string(),
            None,
            None,
            Type::BOOL,
            format0(column_format, 1),
        ),
        FieldInfo::new(
            "Create DB".to_string(),
            None,
            None,
            Type::BOOL,
            format0(column_format, 2),
        ),
        FieldInfo::new(
            "Create user".to_string(),
            None,
            None,
            Type::BOOL,
            format0(column_format, 3),
        ),
    ]
}

fn format0(column_format: Option<&Format>, index: usize) -> FieldFormat {
    column_format
        .map(|format| format.format_for(index))
        .unwrap_or(FieldFormat::Text)
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
            return match statement {
                CatalogStatement::Database(DatabaseStatement::Show(_))
                | CatalogStatement::Schema(SchemaStatement::Show(_))
                | CatalogStatement::Secret(SecretStatement::Show(_)) => {
                    Ok(show_names_fields(column_format))
                }
                CatalogStatement::User(UserStatement::Show(_)) => {
                    Ok(show_users_fields(column_format))
                }
                _ => Ok(Vec::new()),
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
    fn parses_create_database() {
        let statement = parse_catalog_statement("CREATE DATABASE IF NOT EXISTS Analytics")
            .unwrap()
            .unwrap();

        let CatalogStatement::Database(DatabaseStatement::Create(statement)) = statement else {
            panic!("expected CREATE DATABASE")
        };
        assert_eq!(statement.name(), "analytics");
        assert!(statement.if_not_exists());
    }

    #[test]
    fn parses_alter_database_rename() {
        let statement = parse_catalog_statement("ALTER DATABASE analytics RENAME TO warehouse")
            .unwrap()
            .unwrap();

        let CatalogStatement::Database(DatabaseStatement::Alter(statement)) = statement else {
            panic!("expected ALTER DATABASE")
        };
        assert_eq!(statement.name(), "analytics");
        assert_eq!(statement.new_name(), "warehouse");
    }

    #[test]
    fn parses_drop_database() {
        let statement = parse_catalog_statement("DROP DATABASE IF EXISTS analytics")
            .unwrap()
            .unwrap();

        let CatalogStatement::Database(DatabaseStatement::Drop(statement)) = statement else {
            panic!("expected DROP DATABASE")
        };
        assert_eq!(statement.name(), "analytics");
        assert!(statement.if_exists());
    }

    #[test]
    fn parses_show_databases_like_pattern() {
        let statement = parse_catalog_statement("SHOW DATABASES LIKE 'analytics%'")
            .unwrap()
            .unwrap();

        let CatalogStatement::Database(DatabaseStatement::Show(statement)) = statement else {
            panic!("expected SHOW DATABASES")
        };
        assert_eq!(statement.like(), Some("analytics%"));
    }

    #[test]
    fn parses_create_qualified_schema() {
        let statement = parse_catalog_statement("CREATE SCHEMA IF NOT EXISTS Analytics.Events")
            .unwrap()
            .unwrap();

        let CatalogStatement::Schema(SchemaStatement::Create(statement)) = statement else {
            panic!("expected CREATE SCHEMA")
        };
        assert_eq!(statement.schema().database(), Some("analytics"));
        assert_eq!(statement.schema().name(), "events");
        assert!(statement.if_not_exists());
    }

    #[test]
    fn parses_alter_qualified_schema_rename() {
        let statement =
            parse_catalog_statement("ALTER SCHEMA analytics.events RENAME TO archived_events")
                .unwrap()
                .unwrap();

        let CatalogStatement::Schema(SchemaStatement::Alter(statement)) = statement else {
            panic!("expected ALTER SCHEMA")
        };
        assert_eq!(statement.schema().database(), Some("analytics"));
        assert_eq!(statement.schema().name(), "events");
        assert_eq!(statement.new_name(), "archived_events");
    }

    #[test]
    fn parses_drop_schema_cascade() {
        let statement = parse_catalog_statement("DROP SCHEMA IF EXISTS analytics.events CASCADE")
            .unwrap()
            .unwrap();

        let CatalogStatement::Schema(SchemaStatement::Drop(statement)) = statement else {
            panic!("expected DROP SCHEMA")
        };
        assert_eq!(statement.schema().database(), Some("analytics"));
        assert_eq!(statement.schema().name(), "events");
        assert!(statement.if_exists());
        assert!(statement.cascade());
    }

    #[test]
    fn parses_show_schemas_like_pattern() {
        let statement = parse_catalog_statement("SHOW SCHEMAS LIKE 'event%'")
            .unwrap()
            .unwrap();

        let CatalogStatement::Schema(SchemaStatement::Show(statement)) = statement else {
            panic!("expected SHOW SCHEMAS")
        };
        assert_eq!(statement.like(), Some("event%"));
    }

    #[test]
    fn parses_create_user_with_options() {
        let statement = parse_catalog_statement(
            "CREATE USER Alice WITH SUPERUSER CREATEDB NOCREATEUSER PASSWORD 's3cr3t'",
        )
        .unwrap()
        .unwrap();

        let CatalogStatement::User(UserStatement::Create(statement)) = statement else {
            panic!("expected CREATE USER")
        };
        assert_eq!(statement.name(), "alice");
        assert_eq!(statement.options().superuser(), Some(true));
        assert_eq!(statement.options().create_database(), Some(true));
        assert_eq!(statement.options().create_user(), Some(false));
        assert_eq!(
            statement.options().password(),
            &UserPassword::Value("s3cr3t".to_string())
        );
    }

    #[test]
    fn parses_create_user_without_options() {
        let statement = parse_catalog_statement("CREATE USER reader")
            .unwrap()
            .unwrap();

        let CatalogStatement::User(UserStatement::Create(statement)) = statement else {
            panic!("expected CREATE USER")
        };
        assert_eq!(statement.name(), "reader");
        assert_eq!(statement.options(), &UserOptions::default());
    }

    #[test]
    fn rejects_create_user_with_without_options() {
        let error = parse_catalog_statement("CREATE USER reader WITH").unwrap_err();

        assert!(error.to_string().contains("WITH requires"));
    }

    #[test]
    fn parses_alter_user_rename() {
        let statement = parse_catalog_statement("ALTER USER alice RENAME TO admin")
            .unwrap()
            .unwrap();

        let CatalogStatement::User(UserStatement::Alter(statement)) = statement else {
            panic!("expected ALTER USER")
        };
        assert_eq!(statement.name(), "alice");
        assert_eq!(
            statement.action(),
            &AlterUserAction::Rename("admin".to_string())
        );
    }

    #[test]
    fn parses_alter_user_options() {
        let statement =
            parse_catalog_statement("ALTER USER alice WITH NOSUPERUSER CREATEUSER PASSWORD NULL")
                .unwrap()
                .unwrap();

        let CatalogStatement::User(UserStatement::Alter(statement)) = statement else {
            panic!("expected ALTER USER")
        };
        let AlterUserAction::Options(options) = statement.action() else {
            panic!("expected ALTER USER options")
        };
        assert_eq!(options.superuser(), Some(false));
        assert_eq!(options.create_user(), Some(true));
        assert_eq!(options.password(), &UserPassword::Null);
    }

    #[test]
    fn parses_drop_multiple_users() {
        let statement = parse_catalog_statement("DROP USER IF EXISTS alice, Bob")
            .unwrap()
            .unwrap();

        let CatalogStatement::User(UserStatement::Drop(statement)) = statement else {
            panic!("expected DROP USER")
        };
        assert_eq!(statement.names(), &["alice".to_string(), "bob".to_string()]);
        assert!(statement.if_exists());
    }

    #[test]
    fn parses_show_users_like_pattern() {
        let statement = parse_catalog_statement("SHOW USERS LIKE 'admin%'")
            .unwrap()
            .unwrap();

        let CatalogStatement::User(UserStatement::Show(statement)) = statement else {
            panic!("expected SHOW USERS")
        };
        assert_eq!(statement.like(), Some("admin%"));
    }

    #[test]
    fn rejects_duplicate_user_options() {
        let error = parse_catalog_statement("CREATE USER alice SUPERUSER NOSUPERUSER").unwrap_err();

        assert!(error.to_string().contains("specified more than once"));
    }

    #[test]
    fn redacts_user_password_from_debug_output() {
        let statement = parse_catalog_statement("CREATE USER alice PASSWORD 's3cr3t'")
            .unwrap()
            .unwrap();
        let output = format!("{statement:?}");

        assert!(output.contains("[REDACTED]"));
        assert!(!output.contains("s3cr3t"));
    }

    #[test]
    fn rejects_alter_user_without_action() {
        let error = parse_catalog_statement("ALTER USER alice").unwrap_err();

        assert!(error.to_string().contains("requires RENAME TO"));
    }

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
