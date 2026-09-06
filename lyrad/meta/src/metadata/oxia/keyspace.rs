use crate::metadata::{MetadataError, Result};

pub struct Keyspace;

impl Keyspace {
    pub fn new() -> Self {
        Self
    }

    pub fn object(&self, path: &str, name: &str) -> Result<String> {
        self.validate_path(path)?;
        if name.is_empty() || name.contains('/') || name.contains('~') {
            return Err(MetadataError::InvalidKey {
                key: name.to_string(),
                reason: "metadata object names cannot be empty or contain reserved characters",
            });
        }
        Ok(format!("{path}{name}"))
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::path::{CONNECTION_PATH, SOURCE_PATH};

    #[test]
    fn maps_logical_keys_into_the_configured_keyspace() {
        let keyspace = Keyspace::new();

        assert_eq!(
            keyspace.object(CONNECTION_PATH, "kafka").unwrap(),
            "/lyra/v1/connections/kafka"
        );
        assert_eq!(
            keyspace.range(SOURCE_PATH).unwrap(),
            (
                "/lyra/v1/sources/".to_string(),
                "/lyra/v1/sources/~".to_string()
            )
        );
    }

    #[test]
    fn rejects_keys_outside_the_keyspace() {
        let keyspace = Keyspace::new();

        assert!(keyspace.object("connections/", "kafka").is_err());
        assert!(keyspace.object(CONNECTION_PATH, "nested/name").is_err());
    }
}
