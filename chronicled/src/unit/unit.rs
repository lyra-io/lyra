use crate::error::unit_error::UnitError;
use crate::option::unit_options::{ServerOptions, UnitOptions};
use crate::storage::wal::WalOptions;
use crate::storage::{Storage, UnitStorage};
use crate::unit::stream::{RecordStreamContext, run_record_stream};
use chronicle_proto::pb_ext::chronicle_server::{Chronicle, ChronicleServer};
use chronicle_proto::pb_ext::{
    FenceRequest, FenceResponse, FetchEventsRequest, FetchEventsResponse, RecordEventsRequest,
    RecordEventsResponse, StatusCode,
};
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::codegen::BoxStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, warn};

const DEFAULT_INFLIGHT_NUM: usize = 4096;
const RESPONSE_BUFFER: usize = 4;

pub struct Unit {
    inner: Arc<UnitInner>,
    external_handle: JoinHandle<()>,
}

struct UnitInner {
    context: CancellationToken,
    storage: Arc<dyn Storage>,
    stream_handles: Mutex<Vec<JoinHandle<()>>>,
    inflight_capacity: usize,
}

impl UnitInner {
    fn spawn_stream<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(future);
        self.stream_handles.lock().unwrap().push(handle);
    }

    async fn shutdown_streams(&self) {
        self.context.cancel();
        loop {
            let handles = {
                let mut handles = self.stream_handles.lock().unwrap();
                if handles.is_empty() {
                    break;
                }
                std::mem::take(&mut *handles)
            };
            for handle in handles {
                if let Err(err) = handle.await {
                    warn!(error = ?err, "unit stream task join error");
                }
            }
        }
    }
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

        let inner = Arc::new(UnitInner {
            context,
            storage,
            stream_handles: Mutex::new(Vec::new()),
            inflight_capacity: DEFAULT_INFLIGHT_NUM,
        });
        let external_handle = bg_start_external_service(options.server.clone(), inner.clone());

        Ok(Self {
            inner,
            external_handle,
        })
    }

    pub async fn stop(self) {
        info!("unit shutting down");

        self.inner.context.cancel();

        if let Err(err) = self.external_handle.await {
            error!(error = ?err, "unexpected error closing external service");
        }
        self.inner.shutdown_streams().await;
        self.inner.storage.shutdown().await;
        info!("unit stopped");
    }
}

impl UnitInner {
    fn record_stream_context(&self) -> RecordStreamContext {
        RecordStreamContext {
            storage: self.storage.clone(),
            context: self.context.clone(),
            inflight_capacity: self.inflight_capacity,
        }
    }
}

#[tonic::async_trait]
impl Chronicle for UnitInner {
    type RecordStream = BoxStream<RecordEventsResponse>;

    async fn record(
        &self,
        request: Request<Streaming<RecordEventsRequest>>,
    ) -> Result<Response<Self::RecordStream>, Status> {
        let (tx, rx) = mpsc::channel(RESPONSE_BUFFER);
        self.spawn_stream(run_record_stream(
            request.into_inner(),
            tx,
            self.record_stream_context(),
        ));

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as Self::RecordStream))
    }

    type FetchStream = BoxStream<FetchEventsResponse>;

    async fn fetch(
        &self,
        _request: Request<Streaming<FetchEventsRequest>>,
    ) -> Result<Response<Self::FetchStream>, Status> {
        Err(Status::unimplemented("unit read path is disabled"))
    }

    async fn fence(
        &self,
        request: Request<FenceRequest>,
    ) -> Result<Response<FenceResponse>, Status> {
        let req = request.into_inner();
        match self.storage.fence(req.timeline_id, req.term) {
            Ok(lra) => Ok(Response::new(FenceResponse {
                code: StatusCode::Ok.into(),
                lra,
                term: req.term,
            })),
            Err(current_term) => Ok(Response::new(FenceResponse {
                code: StatusCode::Fenced.into(),
                lra: -1,
                term: current_term,
            })),
        }
    }
}

fn bg_start_external_service(options: ServerOptions, inner: Arc<UnitInner>) -> JoinHandle<()> {
    let context = inner.context.clone();
    tokio::spawn(async move {
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<ChronicleServer<UnitInner>>()
            .await;

        info!(addr = %options.bind_address, "grpc service starting");
        let serve_future = Server::builder()
            .add_service(health_service)
            .add_service(ChronicleServer::from_arc(inner))
            .serve_with_shutdown(options.bind_address, context.cancelled());
        info!("unit ready");
        if let Err(err) = serve_future.await {
            error!(error = %err, "grpc service error");
        }
    })
}
