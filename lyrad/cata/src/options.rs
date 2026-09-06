const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 5432;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CataOptions {
    host: String,
    port: u16,
    max_connections: usize,
}

impl CataOptions {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            max_connections: 0,
        }
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
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
    fn defaults_to_postgresql_address() {
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
}
