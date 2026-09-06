use crate::command::CataCommand;
use crate::options::{CataOptions, PostgresOptions};
use crate::{Cata, Result};
use meta::metadata::oxia::{OxiaMetadata, OxiaOptions};
use std::sync::Arc;
use tracing::info;

impl CataCommand {
    pub async fn run(&self) -> Result<()> {
        let _ = tracing_subscriber::fmt().with_target(false).try_init();

        let postgres = PostgresOptions::new(self.host.clone(), self.port)
            .with_max_connections(self.max_connections);
        let oxia = OxiaOptions::new(
            self.oxia_service_address.clone(),
            self.oxia_namespace.clone(),
        );
        let metadata = Arc::new(OxiaMetadata::new(&oxia).await?);
        let options = CataOptions::new(postgres);
        let cata = Cata::new(options, metadata)?;

        info!(
            host = self.host,
            port = self.port,
            oxia_service_address = oxia.service_address(),
            oxia_namespace = oxia.namespace(),
            "starting Cata"
        );
        cata.serve().await
    }
}
