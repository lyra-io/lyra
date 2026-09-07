use std::fmt;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 5432;

#[derive(Clone, PartialEq, Eq)]
pub struct CataOptions {
    host: String,
    port: u16,
    max_connections: usize,
    bootstrap_user: Option<BootstrapUser>,
}

#[derive(Clone, PartialEq, Eq)]
struct BootstrapUser {
    name: String,
    password: String,
}

impl CataOptions {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            max_connections: 0,
            bootstrap_user: None,
        }
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    pub fn with_bootstrap_user(
        mut self,
        name: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.bootstrap_user = Some(BootstrapUser {
            name: name.into(),
            password: password.into(),
        });
        self
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    pub(crate) fn bootstrap_user(&self) -> Option<(&str, &str)> {
        self.bootstrap_user
            .as_ref()
            .map(|user| (user.name.as_str(), user.password.as_str()))
    }
}

impl fmt::Debug for CataOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CataOptions")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("max_connections", &self.max_connections)
            .field(
                "bootstrap_user",
                &self.bootstrap_user.as_ref().map(|user| user.name.as_str()),
            )
            .finish_non_exhaustive()
    }
}

impl Default for CataOptions {
    fn default() -> Self {
        Self::new(DEFAULT_HOST, DEFAULT_PORT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_the_postgresql_address() {
        let options = CataOptions::default();

        assert_eq!(options.host(), "127.0.0.1");
        assert_eq!(options.port(), 5432);
        assert_eq!(options.max_connections(), 0);
    }

    #[test]
    fn constructs_custom_options() {
        let options = CataOptions::new("0.0.0.0", 15432).with_max_connections(128);

        assert_eq!(options.host(), "0.0.0.0");
        assert_eq!(options.port(), 15432);
        assert_eq!(options.max_connections(), 128);
    }

    #[test]
    fn redacts_the_bootstrap_password() {
        let options = CataOptions::default().with_bootstrap_user("root", "s3cr3t");
        let output = format!("{options:?}");

        assert!(output.contains("root"));
        assert!(!output.contains("s3cr3t"));
    }
}
