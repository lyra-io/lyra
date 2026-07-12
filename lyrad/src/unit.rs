use crate::error::unit_error::UnitError;
use crate::grpc::{self, GrpcService};
use crate::option::unit_options::UnitOptions;
use crate::storage::wal::WalOptions;
use crate::storage::{Storage, UnitStorage};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

const DEFAULT_INFLIGHT_NUM: usize = 4096;

pub struct Unit {
    service: GrpcService,
    external_handle: JoinHandle<()>,
}

impl Unit {
    pub async fn new(options: UnitOptions) -> Result<Self, UnitError> {
        info!("unit initializing");
        let context = CancellationToken::new();

        let storage = UnitStorage::open(WalOptions {
            dir: options.wal.dir.clone(),
            max_segment_size: None,
            io_mode: options.io_mode,
        })
        .await?;
        let storage: Arc<dyn Storage> = Arc::new(storage);
        info!(dir = %options.wal.dir, "storage opened");

        let service = GrpcService::new(context, storage, DEFAULT_INFLIGHT_NUM);
        let external_handle = grpc::spawn_server(options.server.clone(), service.clone());

        Ok(Self {
            service,
            external_handle,
        })
    }

    pub async fn stop(self) {
        info!("unit shutting down");

        self.service.cancel();

        if let Err(err) = self.external_handle.await {
            error!(error = ?err, "unexpected error closing external service");
        }
        self.service.shutdown().await;
        info!("unit stopped");
    }
}
