use crate::error::unit_error::UnitError;
use crate::option::auto_config::{AutoConfig, SystemEnv};
use crate::option::unit_options::{ServerOptions, UnitOptions};
use crate::storage::blob::compaction::{CompactionPipeline, CompactionPipelineConfig};
use crate::storage::blob::manager::SegmentManager;
use crate::storage::index::{Storage, StorageOptions};
use crate::storage::write_cache::WriteCache;
use crate::unit::timeline_state::TimelineStateManager;
use crate::unit::unit_service::{UnitService, UnitServiceConfig, UnitServiceTasks};
use crate::wal::checkpoint;
use crate::wal::wal::{Wal, WalOptions};
use chronicle_proto::pb_ext::Event;
use chronicle_proto::pb_ext::chronicle_server::ChronicleServer;
use futures_util::StreamExt;
use prost::Message;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::{error, info, warn};

const DEFAULT_INFLIGHT_NUM: usize = 4096;

pub struct Unit {
    context: CancellationToken,
    external_handle: JoinHandle<()>,
    compaction_pipeline: CompactionPipeline,
    service_tasks: UnitServiceTasks,
    wal: Wal,
}

impl Unit {
    pub async fn new(options: UnitOptions) -> Result<Self, UnitError> {
        info!("unit initializing");
        let context = CancellationToken::new();

        let env = SystemEnv::detect();
        let auto = AutoConfig::from_env_with_io(&env, options.io_mode);

        let resolved_compaction = options.compaction.resolve(&auto);
        let resolved_index = options.index.resolve(&auto);

        let storage = Storage::new(StorageOptions {
            path: options.storage.dir.clone(),
            index: Some(resolved_index),
        })?;
        info!(path = %options.storage.dir, "storage index opened");

        let wal = Wal::new(WalOptions {
            dir: options.wal.dir.clone(),
            max_segment_size: None,
            io_mode: options.io_mode,
        })
        .await?;
        info!(dir = %options.wal.dir, "wal opened");

        let capacity = resolved_compaction.write_cache_capacity_mb * 1024 * 1024;
        let write_cache = WriteCache::new(capacity);

        let remote_store: Option<Arc<dyn crate::storage::blob::remote::RemoteStore>> =
            if let Some(ref offload_opts) = resolved_compaction.offload {
                let s3 = crate::storage::blob::remote::S3RemoteStore::new(
                    offload_opts.bucket.clone(),
                    offload_opts.prefix.clone(),
                    offload_opts.endpoint.clone(),
                    offload_opts.region.clone(),
                )
                .await;
                Some(Arc::new(s3))
            } else {
                None
            };

        let segments_dir = PathBuf::from(&options.segments.dir);
        let segment_manager = Arc::new(SegmentManager::recover_with_remote(
            segments_dir,
            options.io_mode,
            remote_store.clone(),
            64,
            storage.clone(),
        )?);
        info!(dir = %options.segments.dir, "segment manager recovered");

        let wal_checkpoint = checkpoint::read_checkpoint(&storage);
        info!(
            checkpoint_segment = wal_checkpoint.segment_id,
            "wal checkpoint loaded"
        );

        info!("replaying wal into write cache");
        let mut stream = wal.read_stream_from(wal_checkpoint.segment_id).await;
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

        let compaction_pipeline = CompactionPipeline::spawn(CompactionPipelineConfig {
            write_cache: write_cache.clone(),
            segment_manager: segment_manager.clone(),
            index: storage.clone(),
            context: context.clone(),
            interval: Duration::from_millis(resolved_compaction.interval_ms),
            l1_compaction_trigger: resolved_compaction.l1_compaction_trigger,
            l2_compaction_trigger: resolved_compaction.l2_compaction_trigger,
            remote_store,
            wal: Some(wal.clone()),
        });
        info!(
            interval_ms = resolved_compaction.interval_ms,
            "compaction pipeline started"
        );

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
            compaction_pipeline,
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
        self.compaction_pipeline.shutdown().await;
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
