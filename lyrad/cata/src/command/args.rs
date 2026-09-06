use clap::Args;

#[derive(Clone, Debug, Args, PartialEq, Eq)]
pub struct CataCommand {
    #[arg(long, default_value = "127.0.0.1")]
    pub(crate) host: String,

    #[arg(long, default_value_t = 5432)]
    pub(crate) port: u16,

    #[arg(long, default_value_t = 0)]
    pub(crate) max_connections: usize,

    #[arg(long, default_value = "127.0.0.1:6648")]
    pub(crate) oxia_service_address: String,

    #[arg(long, default_value = "default")]
    pub(crate) oxia_namespace: String,
}
