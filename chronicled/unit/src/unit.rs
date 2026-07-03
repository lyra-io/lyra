use crate::error::unit_error::UnitError;
use crate::option::unit_options::{ServerOptions, UnitOptions};
use crate::storage::wal::WalOptions;
use crate::storage::{Storage, UnitStorage};
use crate::unit_service::UnitService;
use chronicle_proto::pb_ext::chronicle_server::ChronicleServer;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::{error, info};

const DEFAULT_INFLIGHT_NUM: usize = 4096;

pub struct Unit {
    service: UnitService,
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

        let service = UnitService::new(context, storage, DEFAULT_INFLIGHT_NUM);
        let external_handle = bg_start_external_service(options.server.clone(), service.clone());

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

fn bg_start_external_service(options: ServerOptions, service: UnitService) -> JoinHandle<()> {
    let context = service.context();
    tokio::spawn(async move {
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<ChronicleServer<UnitService>>()
            .await;

        info!(addr = %options.bind_address, "grpc service starting");
        let serve_future = Server::builder()
            .add_service(health_service)
            .add_service(ChronicleServer::new(service))
            .serve_with_shutdown(options.bind_address, context.cancelled());
        info!("unit ready");
        if let Err(err) = serve_future.await {
            error!(error = %err, "grpc service error");
        }
    })
}
