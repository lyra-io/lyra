use crate::error::unit_error::UnitError;
use crate::option::unit_options::{ServerOptions, UnitOptions};
use crate::storage::write_cache::WriteCache;
use crate::unit::timeline_state::TimelineStateManager;
use crate::unit::unit_service::{UnitService, UnitServiceConfig, UnitServiceTasks};
use crate::wal::wal::{Wal, WalOptions};
use chronicle_proto::pb_ext::Event;
use chronicle_proto::pb_ext::chronicle_server::ChronicleServer;
use futures_util::StreamExt;
use prost::Message;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::{error, info, warn};

const DEFAULT_INFLIGHT_NUM: usize = 4096;

pub struct Unit {
    context: CancellationToken,
    external_handle: JoinHandle<()>,
    service_tasks: UnitServiceTasks,
    wal: Wal,
}

impl Unit {
    pub async fn new(options: UnitOptions) -> Result<Self, UnitError> {
        info!("unit initializing");
        let context = CancellationToken::new();

        let wal = Wal::new(WalOptions {
            dir: options.wal.dir.clone(),
            max_segment_size: None,
            io_mode: options.io_mode,
        })
        .await?;
        info!(dir = %options.wal.dir, "wal opened");

        let write_cache = WriteCache::new();

        info!("replaying wal into write cache");
        let mut stream = wal.read_stream().await;
        let mut replayed = 0u64;
        while let Some(result) = stream.next().await {
            match result {
                Ok(data) => {
                    if let Ok(event) = Event::decode(data.as_slice()) {
                        write_cache.put_direct(event, false);
                        replayed += 1;
                    }
                }
                Err(e) => {
                    warn!(error = ?e, "wal replay error reading record");
                    break;
                }
            }
        }
        drop(stream);
        info!(events = replayed, "wal replay complete");

        let timeline_state = Arc::new(TimelineStateManager::new());

        let service_tasks = UnitServiceTasks::new(context.clone());

        let unit_service = UnitService::new(UnitServiceConfig {
            wal: wal.clone(),
            write_cache: write_cache.clone(),
            timeline_state: timeline_state.clone(),
            tasks: service_tasks.clone(),
            inflight_capacity: DEFAULT_INFLIGHT_NUM,
        });

        let external_handle =
            bg_start_external_service(options.server.clone(), context.clone(), unit_service);

        Ok(Self {
            context,
            external_handle,
            service_tasks,
            wal,
        })
    }

    pub async fn stop(self) {
        info!("unit shutting down");

        self.context.cancel();

        if let Err(err) = self.external_handle.await {
            error!(error = ?err, "unexpected error closing external service");
        }
        self.service_tasks.shutdown().await;
        self.wal.shutdown().await;
        info!("unit stopped");
    }
}

fn bg_start_external_service(
    options: ServerOptions,
    context: CancellationToken,
    unit_service: UnitService,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<ChronicleServer<UnitService>>()
            .await;

        info!(addr = %options.bind_address, "grpc service starting");
        let serve_future = Server::builder()
            .add_service(health_service)
            .add_service(ChronicleServer::new(unit_service))
            .serve_with_shutdown(options.bind_address, context.cancelled());
        info!("unit ready");
        if let Err(err) = serve_future.await {
            error!(error = %err, "grpc service error");
        }
    })
}
