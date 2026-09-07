use crate::metadata::{MetadataError, Result};

pub struct Keyspace;

impl Keyspace {
    pub fn new() -> Self {
        Self
    }

    pub fn object(&self, path: &str, name: &str) -> Result<String> {
        self.validate_path(path)?;
        self.validate_name(name)?;
        Ok(format!("{path}{name}"))
    }

    pub fn collection(&self, path: &str, name: &str, collection: &str) -> Result<String> {
        self.validate_path(path)?;
        self.validate_name(name)?;
        if collection.is_empty()
            || collection.starts_with('/')
            || !collection.ends_with('/')
            || collection.contains("//")
            || collection.contains('~')
        {
            return Err(MetadataError::InvalidKey {
                key: collection.to_string(),
                reason: "metadata collection paths must be relative constants ending in a slash",
            });
        }
        Ok(format!("{path}{name}/{collection}"))
    }

    pub fn range(&self, path: &str) -> Result<(String, String)> {
        self.validate_path(path)?;
        Ok((path.to_string(), format!("{path}~")))
    }

    fn validate_path(&self, path: &str) -> Result<()> {
        if !path.starts_with('/')
            || !path.ends_with('/')
            || path.contains("//")
            || path.contains('~')
        {
            return Err(MetadataError::InvalidKey {
                key: path.to_string(),
                reason: "metadata paths must be absolute constants ending in a slash",
            });
        }
        Ok(())
    }

    fn validate_name(&self, name: &str) -> Result<()> {
        if name.is_empty() || name.contains('/') || name.contains('~') {
            return Err(MetadataError::InvalidKey {
                key: name.to_string(),
                reason: "metadata object names cannot be empty or contain reserved characters",
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::path::{DATABASE_PATH, SCHEMA_PATH, SOURCE_PATH, USER_PATH};

    #[test]
    fn maps_logical_keys_into_the_configured_keyspace() {
        let keyspace = Keyspace::new();

        assert_eq!(
            keyspace.object(USER_PATH, "root").unwrap(),
            "/lyra/v1/users/root"
        );
        assert_eq!(
            keyspace.object(DATABASE_PATH, "dev").unwrap(),
            "/lyra/v1/databases/dev"
        );
        assert_eq!(
            keyspace
                .collection(DATABASE_PATH, "dev", SCHEMA_PATH)
                .unwrap(),
            "/lyra/v1/databases/dev/schemas/"
        );
        assert_eq!(
            keyspace
                .collection("/lyra/v1/databases/dev/schemas/", "public", SOURCE_PATH)
                .unwrap(),
            "/lyra/v1/databases/dev/schemas/public/sources/"
        );
        assert_eq!(
            keyspace
                .range("/lyra/v1/databases/dev/schemas/public/sources/")
                .unwrap(),
            (
                "/lyra/v1/databases/dev/schemas/public/sources/".to_string(),
                "/lyra/v1/databases/dev/schemas/public/sources/~".to_string()
            )
        );
    }

    #[test]
    fn rejects_keys_outside_the_keyspace() {
        let keyspace = Keyspace::new();

        assert!(keyspace.object("databases/", "dev").is_err());
        assert!(keyspace.object(DATABASE_PATH, "nested/name").is_err());
        assert!(
            keyspace
                .collection(DATABASE_PATH, "dev", "/secrets/")
                .is_err()
        );
    }
}
