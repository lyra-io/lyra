use crate::option::unit_options::ServerOptions;
use crate::storage::Storage;
use futures_util::{Stream, StreamExt};
use lyra_proto::pb_ext::lyra_server::Lyra;
use lyra_proto::pb_ext::lyra_server::LyraServer;
use lyra_proto::pb_ext::{
    AppendEventsRequest, AppendEventsResponse, ChunkType, FenceRequest, FenceResponse,
    ReadEventsRequest, ReadEventsResponse, StatusCode,
};
use prost::Message;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::codegen::BoxStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{error, info, warn};

const RESPONSE_BUFFER: usize = 4;
const READ_CHUNK_SIZE: usize = 1024;

#[derive(Clone)]
pub struct GrpcService {
    context: CancellationToken,
    storage: Arc<dyn Storage>,
    stream_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    inflight_capacity: usize,
}

impl GrpcService {
    pub fn new(
        context: CancellationToken,
        storage: Arc<dyn Storage>,
        inflight_capacity: usize,
    ) -> Self {
        Self {
            context,
            storage,
            stream_handles: Arc::new(Mutex::new(Vec::new())),
            inflight_capacity,
        }
    }

    pub fn context(&self) -> CancellationToken {
        self.context.clone()
    }

    pub fn cancel(&self) {
        self.context.cancel();
    }

    fn spawn_stream<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(future);
        self.stream_handles.lock().unwrap().push(handle);
    }

    pub async fn shutdown(&self) {
        self.cancel();
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
                    warn!(error = ?err, "grpc stream task join error");
                }
            }
        }
        self.storage.shutdown().await;
    }

    fn append_stream_context(&self) -> AppendStreamContext {
        AppendStreamContext {
            storage: self.storage.clone(),
            context: self.context.clone(),
            inflight_capacity: self.inflight_capacity,
        }
    }

    fn read_stream_context(&self) -> ReadStreamContext {
        ReadStreamContext {
            storage: self.storage.clone(),
            context: self.context.clone(),
        }
    }
}

pub fn spawn_server(options: ServerOptions, service: GrpcService) -> JoinHandle<()> {
    let context = service.context();
    tokio::spawn(async move {
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<LyraServer<GrpcService>>()
            .await;

        info!(addr = %options.bind_address, "grpc service starting");
        let serve_future = Server::builder()
            .add_service(health_service)
            .add_service(LyraServer::new(service))
            .serve_with_shutdown(options.bind_address, context.cancelled());
        info!("grpc service ready");
        if let Err(err) = serve_future.await {
            error!(error = %err, "grpc service error");
        }
    })
}

#[tonic::async_trait]
impl Lyra for GrpcService {
    type AppendStream = BoxStream<AppendEventsResponse>;

    async fn append(
        &self,
        request: Request<Streaming<AppendEventsRequest>>,
    ) -> Result<Response<Self::AppendStream>, Status> {
        let (tx, rx) = mpsc::channel(RESPONSE_BUFFER);
        self.spawn_stream(run_append_stream(
            request.into_inner(),
            tx,
            self.append_stream_context(),
        ));

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as Self::AppendStream))
    }

    type ReadStream = BoxStream<ReadEventsResponse>;

    async fn read(
        &self,
        request: Request<Streaming<ReadEventsRequest>>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let (tx, rx) = mpsc::channel(RESPONSE_BUFFER);
        self.spawn_stream(run_read_stream(
            request.into_inner(),
            tx,
            self.read_stream_context(),
        ));

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as Self::ReadStream))
    }

    async fn fence(
        &self,
        request: Request<FenceRequest>,
    ) -> Result<Response<FenceResponse>, Status> {
        let req = request.into_inner();
        match self.storage.fence(req.stream_id, req.term) {
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

#[derive(Clone)]
struct AppendStreamContext {
    storage: Arc<dyn Storage>,
    context: CancellationToken,
    inflight_capacity: usize,
}

struct InflightWrite {
    wal_offset: i64,
    event: lyra_proto::pb_ext::Event,
    trunc: bool,
    ack: Arc<BatchAck>,
}

struct BatchAck {
    response_tx: mpsc::Sender<Result<AppendEventsResponse, Status>>,
    stream_id: i64,
    term: i64,
    state: AsyncMutex<BatchAckState>,
}

struct BatchAckState {
    remaining: usize,
    max_offset: i64,
    completed: bool,
}

impl BatchAck {
    fn new(
        response_tx: mpsc::Sender<Result<AppendEventsResponse, Status>>,
        stream_id: i64,
        term: i64,
        item_count: usize,
    ) -> Self {
        Self {
            response_tx,
            stream_id,
            term,
            state: AsyncMutex::new(BatchAckState {
                remaining: item_count,
                max_offset: -1,
                completed: false,
            }),
        }
    }

    async fn complete_ok(&self, offset: i64) {
        let response = {
            let mut state = self.state.lock().await;
            if state.completed {
                return;
            }
            state.max_offset = state.max_offset.max(offset);
            state.remaining = state.remaining.saturating_sub(1);
            if state.remaining == 0 {
                state.completed = true;
                Some(Ok(AppendEventsResponse {
                    code: StatusCode::Ok.into(),
                    commit_offset: state.max_offset,
                    stream_id: self.stream_id,
                    term: self.term,
                }))
            } else {
                None
            }
        };
        if let Some(response) = response {
            self.send_completion(response).await;
        }
    }

    async fn fail_status(&self, status: Status) {
        let response = {
            let mut state = self.state.lock().await;
            if state.completed {
                return;
            }
            state.completed = true;
            Err(status)
        };
        self.send_completion(response).await;
    }

    async fn send_completion(&self, response: Result<AppendEventsResponse, Status>) {
        let _ = self.response_tx.send(response).await;
    }
}

async fn run_append_stream<S>(
    stream: S,
    response_tx: mpsc::Sender<Result<AppendEventsResponse, Status>>,
    context: AppendStreamContext,
) where
    S: Stream<Item = Result<AppendEventsRequest, Status>> + Send + Unpin + 'static,
{
    let (inflight_tx, inflight_rx) = mpsc::channel(context.inflight_capacity);
    let synced_watch = context.storage.watch_synced();
    let receive_loop =
        receive_append_requests(stream, response_tx.clone(), inflight_tx, context.clone());
    let sync_loop =
        sync_append_inflight(inflight_rx, context.storage, context.context, synced_watch);
    tokio::join!(receive_loop, sync_loop);
}

async fn receive_append_requests<S>(
    mut stream: S,
    response_tx: mpsc::Sender<Result<AppendEventsResponse, Status>>,
    inflight_tx: mpsc::Sender<InflightWrite>,
    context: AppendStreamContext,
) where
    S: Stream<Item = Result<AppendEventsRequest, Status>> + Unpin,
{
    loop {
        let request = tokio::select! {
            _ = context.context.cancelled() => break,
            request = stream.next() => request,
        };

        match request {
            Some(Ok(request)) => {
                if response_tx.is_closed() {
                    break;
                }
                enqueue_append_batch(
                    request,
                    response_tx.clone(),
                    inflight_tx.clone(),
                    context.clone(),
                )
                .await;
            }
            Some(Err(status)) => {
                let _ = response_tx.send(Err(status)).await;
                break;
            }
            None => break,
        }
    }
}

async fn enqueue_append_batch(
    request: AppendEventsRequest,
    response_tx: mpsc::Sender<Result<AppendEventsResponse, Status>>,
    inflight_tx: mpsc::Sender<InflightWrite>,
    context: AppendStreamContext,
) {
    let item_count = request.items.len();

    if item_count == 0 {
        let _ = response_tx
            .send(Err(Status::invalid_argument("empty append batch")))
            .await;
        return;
    }

    let mut writes = Vec::with_capacity(item_count);
    let mut batch_stream_id = None;
    let mut batch_term = None;

    for item in request.items {
        let event = match item.event {
            Some(event) => event,
            None => {
                let _ = response_tx
                    .send(Err(Status::invalid_argument("append item missing event")))
                    .await;
                return;
            }
        };

        if let Some(stream_id) = batch_stream_id {
            if stream_id != event.stream_id || batch_term != Some(event.term) {
                let _ = response_tx
                    .send(Err(Status::invalid_argument(
                        "append batch must contain one stream and term",
                    )))
                    .await;
                return;
            }
        } else {
            batch_stream_id = Some(event.stream_id);
            batch_term = Some(event.term);
        }

        if let Err(current_term) = context.storage.check_term(event.stream_id, event.term) {
            let _ = response_tx
                .send(Ok(AppendEventsResponse {
                    code: StatusCode::InvalidTerm.into(),
                    commit_offset: -1,
                    stream_id: event.stream_id,
                    term: current_term,
                }))
                .await;
            return;
        }

        writes.push((event, item.trunc));
    }

    let stream_id = batch_stream_id.unwrap_or_default();
    let term = batch_term.unwrap_or_default();
    let ack = Arc::new(BatchAck::new(response_tx, stream_id, term, item_count));

    for (event, trunc) in writes {
        let permit = tokio::select! {
            _ = context.context.cancelled() => {
                ack.fail_status(Status::cancelled("append stream cancelled")).await;
                return;
            }
            permit = inflight_tx.reserve() => match permit {
                Ok(permit) => permit,
                Err(_) => {
                    ack.fail_status(Status::unavailable("append stream closed")).await;
                    return;
                }
            },
        };

        let encoded = event.encode_to_vec();
        let wal_offset = tokio::select! {
            _ = context.context.cancelled() => {
                ack.fail_status(Status::cancelled("append stream cancelled")).await;
                return;
            }
            result = context.storage.append(encoded) => match result {
                Ok(offset) => offset,
                Err(error) => {
                    ack.fail_status(Status::internal(error.to_string())).await;
                    return;
                }
            },
        };

        permit.send(InflightWrite {
            wal_offset,
            event,
            trunc,
            ack: ack.clone(),
        });
    }
}

async fn sync_append_inflight(
    mut inflight_rx: mpsc::Receiver<InflightWrite>,
    storage: Arc<dyn Storage>,
    context: CancellationToken,
    mut watch: tokio::sync::watch::Receiver<i64>,
) {
    let mut synced_offset = *watch.borrow();
    let mut pending = VecDeque::new();
    let mut inflight_closed = false;

    loop {
        synced_offset = synced_offset.max(*watch.borrow());
        if !drain_synced_writes(&mut pending, synced_offset, &storage, &context).await {
            fail_pending_and_close(&mut pending, &mut inflight_rx).await;
            break;
        }

        if inflight_closed && pending.is_empty() {
            break;
        }

        tokio::select! {
            _ = context.cancelled() => {
                fail_pending_and_close(&mut pending, &mut inflight_rx).await;
                break;
            }
            changed = watch.changed() => {
                match changed {
                    Ok(()) => synced_offset = *watch.borrow(),
                    Err(_) => {
                        fail_pending_writes(&mut pending, Status::internal("wal sync watch closed")).await;
                        break;
                    }
                }
            }
            write = inflight_rx.recv(), if !inflight_closed => {
                match write {
                    Some(write) => pending.push_back(write),
                    None => inflight_closed = true,
                }
            }
        }
    }
}

async fn drain_synced_writes(
    pending: &mut VecDeque<InflightWrite>,
    synced_offset: i64,
    storage: &Arc<dyn Storage>,
    context: &CancellationToken,
) -> bool {
    while pending
        .front()
        .is_some_and(|write| write.wal_offset <= synced_offset)
    {
        let write = pending.pop_front().unwrap();
        let stream_id = write.event.stream_id;
        let offset = write.event.offset;
        let trunc = write.trunc;
        tokio::select! {
            _ = context.cancelled() => {
                write.ack.fail_status(Status::cancelled("append stream cancelled")).await;
                return false;
            }
            _ = storage.apply_write(write.event, trunc) => {}
        }
        storage.update_lra(stream_id, offset);
        write.ack.complete_ok(offset).await;
    }
    true
}

async fn fail_pending_and_close(
    pending: &mut VecDeque<InflightWrite>,
    inflight_rx: &mut mpsc::Receiver<InflightWrite>,
) {
    inflight_rx.close();
    let status = Status::cancelled("append stream cancelled");
    fail_pending_writes(pending, status.clone()).await;
    while let Some(write) = inflight_rx.recv().await {
        write.ack.fail_status(status.clone()).await;
    }
}

async fn fail_pending_writes(pending: &mut VecDeque<InflightWrite>, status: Status) {
    while let Some(write) = pending.pop_front() {
        write.ack.fail_status(status.clone()).await;
    }
}

#[derive(Clone)]
struct ReadStreamContext {
    storage: Arc<dyn Storage>,
    context: CancellationToken,
}

async fn run_read_stream<S>(
    mut stream: S,
    response_tx: mpsc::Sender<Result<ReadEventsResponse, Status>>,
    context: ReadStreamContext,
) where
    S: Stream<Item = Result<ReadEventsRequest, Status>> + Send + Unpin + 'static,
{
    loop {
        let request = tokio::select! {
            _ = context.context.cancelled() => break,
            request = stream.next() => request,
        };

        match request {
            Some(Ok(request)) => {
                if response_tx.is_closed() {
                    break;
                }
                if let Err(status) =
                    handle_read_request(request, response_tx.clone(), context.clone()).await
                {
                    let _ = response_tx.send(Err(status)).await;
                    break;
                }
            }
            Some(Err(status)) => {
                let _ = response_tx.send(Err(status)).await;
                break;
            }
            None => break,
        }
    }
}

async fn handle_read_request(
    request: ReadEventsRequest,
    response_tx: mpsc::Sender<Result<ReadEventsResponse, Status>>,
    context: ReadStreamContext,
) -> Result<(), Status> {
    if request.end_offset < request.start_offset {
        return Err(Status::invalid_argument(
            "read end_offset must be greater than or equal to start_offset",
        ));
    }

    let events = context
        .storage
        .read_events(request.stream_id, request.start_offset, request.end_offset)
        .await
        .map_err(|error| Status::internal(error.to_string()))?;

    if events.is_empty() {
        send_read_response(
            &response_tx,
            ReadEventsResponse {
                code: StatusCode::Ok.into(),
                r#type: ChunkType::Full.into(),
                stream_id: request.stream_id,
                event: Vec::new(),
                advanced_offset: request.start_offset,
            },
            &context.context,
        )
        .await?;
        return Ok(());
    }

    let chunk_count = events.len().div_ceil(READ_CHUNK_SIZE);
    for (index, chunk) in events.chunks(READ_CHUNK_SIZE).enumerate() {
        let chunk_type = if chunk_count == 1 {
            ChunkType::Full
        } else if index == 0 {
            ChunkType::First
        } else if index + 1 == chunk_count {
            ChunkType::Last
        } else {
            ChunkType::Middle
        };
        let advanced_offset = chunk
            .last()
            .map(|event| event.offset + 1)
            .unwrap_or(request.start_offset);

        send_read_response(
            &response_tx,
            ReadEventsResponse {
                code: StatusCode::Ok.into(),
                r#type: chunk_type.into(),
                stream_id: request.stream_id,
                event: chunk.to_vec(),
                advanced_offset,
            },
            &context.context,
        )
        .await?;
    }

    Ok(())
}

async fn send_read_response(
    response_tx: &mpsc::Sender<Result<ReadEventsResponse, Status>>,
    response: ReadEventsResponse,
    context: &CancellationToken,
) -> Result<(), Status> {
    tokio::select! {
        _ = context.cancelled() => Err(Status::cancelled("read stream cancelled")),
        result = response_tx.send(Ok(response)) => result
            .map_err(|_| Status::cancelled("read response stream closed")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::wal::WalOptions;
    use crate::storage::{Storage, UnitStorage};
    use futures_util::StreamExt;
    use lyra_proto::pb_ext::{AppendEventsRequestItem, Event, lyra_client::LyraClient};
    use tempfile::tempdir;

    fn event(stream_id: i64, offset: i64, payload: &[u8]) -> Event {
        Event {
            stream_id,
            term: 1,
            offset,
            payload: Some(payload.to_vec().into()),
            crc32: None,
            timestamp: offset * 10,
            schema_id: 0,
        }
    }

    async fn test_storage() -> Arc<dyn Storage> {
        let dir = tempdir().unwrap();
        Arc::new(
            UnitStorage::open(WalOptions {
                dir: dir.path().to_string_lossy().into_owned(),
                max_segment_size: None,
                io_mode: Default::default(),
            })
            .await
            .unwrap(),
        )
    }

    async fn collect_read(
        storage: Arc<dyn Storage>,
        requests: Vec<ReadEventsRequest>,
    ) -> Vec<Result<ReadEventsResponse, Status>> {
        let (tx, rx) = mpsc::channel(RESPONSE_BUFFER);
        let context = ReadStreamContext {
            storage,
            context: CancellationToken::new(),
        };
        let request_stream = tokio_stream::iter(requests.into_iter().map(Ok));
        run_read_stream(request_stream, tx, context).await;
        ReceiverStream::new(rx).collect().await
    }

    #[tokio::test]
    async fn read_returns_events_in_requested_range() {
        let storage = test_storage().await;
        storage.apply_write(event(7, 1, b"a"), false).await;
        storage.apply_write(event(7, 2, b"b"), false).await;
        storage.apply_write(event(7, 3, b"c"), false).await;
        storage.apply_write(event(8, 1, b"other"), false).await;

        let responses = collect_read(
            storage,
            vec![ReadEventsRequest {
                stream_id: 7,
                start_offset: 2,
                end_offset: 4,
            }],
        )
        .await;

        assert_eq!(responses.len(), 1);
        let response = responses.into_iter().next().unwrap().unwrap();
        assert_eq!(response.code, StatusCode::Ok as i32);
        assert_eq!(response.r#type, ChunkType::Full as i32);
        assert_eq!(response.advanced_offset, 4);
        assert_eq!(
            response
                .event
                .iter()
                .map(|event| event.offset)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[tokio::test]
    async fn read_empty_range_returns_full_empty_response() {
        let storage = test_storage().await;

        let responses = collect_read(
            storage,
            vec![ReadEventsRequest {
                stream_id: 7,
                start_offset: 10,
                end_offset: 20,
            }],
        )
        .await;

        assert_eq!(responses.len(), 1);
        let response = responses.into_iter().next().unwrap().unwrap();
        assert_eq!(response.code, StatusCode::Ok as i32);
        assert_eq!(response.r#type, ChunkType::Full as i32);
        assert_eq!(response.advanced_offset, 10);
        assert!(response.event.is_empty());
    }

    #[tokio::test]
    async fn read_rejects_invalid_range() {
        let storage = test_storage().await;

        let responses = collect_read(
            storage,
            vec![ReadEventsRequest {
                stream_id: 7,
                start_offset: 20,
                end_offset: 10,
            }],
        )
        .await;

        assert_eq!(responses.len(), 1);
        let status = responses.into_iter().next().unwrap().unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn append_then_read_round_trips_through_unit_streams() {
        let storage = test_storage().await;
        let (append_tx, append_rx) = mpsc::channel(RESPONSE_BUFFER);
        let append_cancel = CancellationToken::new();
        let append_context = AppendStreamContext {
            storage: storage.clone(),
            context: append_cancel.clone(),
            inflight_capacity: 16,
        };
        let append_request = AppendEventsRequest {
            items: vec![AppendEventsRequestItem {
                event: Some(event(7, 1, b"a")),
                trunc: false,
                lra: 0,
            }],
        };
        let append_stream = tokio_stream::iter(vec![Ok(append_request)]);

        let append_handle =
            tokio::spawn(run_append_stream(append_stream, append_tx, append_context));

        let mut append_responses = ReceiverStream::new(append_rx);
        let append_response =
            tokio::time::timeout(std::time::Duration::from_secs(5), append_responses.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        assert_eq!(append_response.code, StatusCode::Ok as i32);
        assert_eq!(append_response.commit_offset, 1);
        append_cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(5), append_handle)
            .await
            .unwrap()
            .unwrap();

        let read_responses = collect_read(
            storage,
            vec![ReadEventsRequest {
                stream_id: 7,
                start_offset: 1,
                end_offset: 2,
            }],
        )
        .await;

        assert_eq!(read_responses.len(), 1);
        let read_response = read_responses.into_iter().next().unwrap().unwrap();
        assert_eq!(read_response.event.len(), 1);
        assert_eq!(read_response.event[0].offset, 1);
        assert_eq!(read_response.event[0].payload.as_deref(), Some(&b"a"[..]));
    }

    #[tokio::test]
    async fn append_then_read_round_trips_over_tonic_service() {
        let storage = test_storage().await;
        let context = CancellationToken::new();
        let service = GrpcService::new(context.clone(), storage, 16);
        let server_service = service.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = async_stream::stream! {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => yield Ok::<_, std::io::Error>(stream),
                    Err(error) => {
                        yield Err(error);
                        break;
                    }
                }
            }
        };

        let server_handle = tokio::spawn(async move {
            Server::builder()
                .add_service(LyraServer::new(server_service))
                .serve_with_incoming_shutdown(incoming, context.cancelled())
                .await
                .unwrap();
        });

        let mut client = LyraClient::connect(format!("http://{}", addr))
            .await
            .unwrap();

        let (append_tx, append_rx) = mpsc::channel(4);
        let mut append_responses = client
            .append(ReceiverStream::new(append_rx))
            .await
            .unwrap()
            .into_inner();
        append_tx
            .send(AppendEventsRequest {
                items: vec![AppendEventsRequestItem {
                    event: Some(event(7, 1, b"a")),
                    trunc: false,
                    lra: 0,
                }],
            })
            .await
            .unwrap();

        let append_response = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            append_responses.message(),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(append_response.code, StatusCode::Ok as i32);
        assert_eq!(append_response.commit_offset, 1);

        let (read_tx, read_rx) = mpsc::channel(4);
        let mut read_responses = client
            .read(ReceiverStream::new(read_rx))
            .await
            .unwrap()
            .into_inner();
        read_tx
            .send(ReadEventsRequest {
                stream_id: 7,
                start_offset: 1,
                end_offset: 2,
            })
            .await
            .unwrap();

        let read_response =
            tokio::time::timeout(std::time::Duration::from_secs(5), read_responses.message())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        assert_eq!(read_response.code, StatusCode::Ok as i32);
        assert_eq!(read_response.r#type, ChunkType::Full as i32);
        assert_eq!(read_response.event.len(), 1);
        assert_eq!(read_response.event[0].offset, 1);
        assert_eq!(read_response.event[0].payload.as_deref(), Some(&b"a"[..]));

        service.shutdown().await;
        tokio::time::timeout(std::time::Duration::from_secs(5), server_handle)
            .await
            .unwrap()
            .unwrap();
    }
}
