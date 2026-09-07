use datafusion_postgres::pgwire::api::{ClientInfo, METADATA_DATABASE, METADATA_USER};
use meta::metadata::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

pub(crate) fn client_database<C>(client: &C) -> &str
where
    C: ClientInfo,
{
    client
        .metadata()
        .get(METADATA_DATABASE)
        .map(String::as_str)
        .unwrap_or(DEFAULT_DATABASE_NAME)
}

pub(crate) fn client_schemas<C>(client: &C) -> Vec<String>
where
    C: ClientInfo,
{
    let schemas = client
        .metadata()
        .get("search_path")
        .into_iter()
        .flat_map(|search_path| search_path.split(','))
        .map(str::trim)
        .map(|schema| schema.trim_matches('"'))
        .filter(|schema| !schema.is_empty() && *schema != "$user")
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    if schemas.is_empty() {
        vec![DEFAULT_SCHEMA_NAME.to_string()]
    } else {
        schemas
    }
}

pub(crate) fn client_user<C>(client: &C) -> Option<&str>
where
    C: ClientInfo,
{
    client.metadata().get(METADATA_USER).map(String::as_str)
}
