use crate::storage::Storage;
use futures_util::{Stream, StreamExt};
use lyra_proto::pb_ext::lyra_server::Lyra;
use lyra_proto::pb_ext::{
    ChunkType, FenceRequest, FenceResponse, FetchEventsRequest, FetchEventsResponse,
    RecordEventsRequest, RecordEventsResponse, StatusCode,
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
use tonic::{Request, Response, Status, Streaming};
use tracing::warn;

const RESPONSE_BUFFER: usize = 4;
const FETCH_CHUNK_SIZE: usize = 1024;

#[derive(Clone)]
pub(crate) struct UnitService {
    context: CancellationToken,
    storage: Arc<dyn Storage>,
    stream_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    inflight_capacity: usize,
}

impl UnitService {
    pub(crate) fn new(
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

    pub(crate) fn context(&self) -> CancellationToken {
        self.context.clone()
    }

    pub(crate) fn cancel(&self) {
        self.context.cancel();
    }

    fn spawn_stream<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(future);
        self.stream_handles.lock().unwrap().push(handle);
    }

    pub(crate) async fn shutdown(&self) {
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
                    warn!(error = ?err, "unit stream task join error");
                }
            }
        }
        self.storage.shutdown().await;
    }

    fn record_stream_context(&self) -> RecordStreamContext {
        RecordStreamContext {
            storage: self.storage.clone(),
            context: self.context.clone(),
            inflight_capacity: self.inflight_capacity,
        }
    }

    fn fetch_stream_context(&self) -> FetchStreamContext {
        FetchStreamContext {
            storage: self.storage.clone(),
            context: self.context.clone(),
        }
    }
}

#[tonic::async_trait]
impl Lyra for UnitService {
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
        request: Request<Streaming<FetchEventsRequest>>,
    ) -> Result<Response<Self::FetchStream>, Status> {
        let (tx, rx) = mpsc::channel(RESPONSE_BUFFER);
        self.spawn_stream(run_fetch_stream(
            request.into_inner(),
            tx,
            self.fetch_stream_context(),
        ));

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as Self::FetchStream))
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

#[derive(Clone)]
struct RecordStreamContext {
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
    response_tx: mpsc::Sender<Result<RecordEventsResponse, Status>>,
    timeline_id: i64,
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
        response_tx: mpsc::Sender<Result<RecordEventsResponse, Status>>,
        timeline_id: i64,
        term: i64,
        item_count: usize,
    ) -> Self {
        Self {
            response_tx,
            timeline_id,
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
                Some(Ok(RecordEventsResponse {
                    code: StatusCode::Ok.into(),
                    commit_offset: state.max_offset,
                    timeline_id: self.timeline_id,
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

    async fn send_completion(&self, response: Result<RecordEventsResponse, Status>) {
        let _ = self.response_tx.send(response).await;
    }
}

async fn run_record_stream<S>(
    stream: S,
    response_tx: mpsc::Sender<Result<RecordEventsResponse, Status>>,
    context: RecordStreamContext,
) where
    S: Stream<Item = Result<RecordEventsRequest, Status>> + Send + Unpin + 'static,
{
    let (inflight_tx, inflight_rx) = mpsc::channel(context.inflight_capacity);
    let synced_watch = context.storage.watch_synced();
    let receive_loop =
        receive_record_requests(stream, response_tx.clone(), inflight_tx, context.clone());
    let sync_loop =
        sync_record_inflight(inflight_rx, context.storage, context.context, synced_watch);
    tokio::join!(receive_loop, sync_loop);
}

async fn receive_record_requests<S>(
    mut stream: S,
    response_tx: mpsc::Sender<Result<RecordEventsResponse, Status>>,
    inflight_tx: mpsc::Sender<InflightWrite>,
    context: RecordStreamContext,
) where
    S: Stream<Item = Result<RecordEventsRequest, Status>> + Unpin,
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
                enqueue_record_batch(
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

async fn enqueue_record_batch(
    request: RecordEventsRequest,
    response_tx: mpsc::Sender<Result<RecordEventsResponse, Status>>,
    inflight_tx: mpsc::Sender<InflightWrite>,
    context: RecordStreamContext,
) {
    let item_count = request.items.len();

    if item_count == 0 {
        let _ = response_tx
            .send(Err(Status::invalid_argument("empty record batch")))
            .await;
        return;
    }

    let mut writes = Vec::with_capacity(item_count);
    let mut batch_timeline_id = None;
    let mut batch_term = None;

    for item in request.items {
        let event = match item.event {
            Some(event) => event,
            None => {
                let _ = response_tx
                    .send(Err(Status::invalid_argument("record item missing event")))
                    .await;
                return;
            }
        };

        if let Some(timeline_id) = batch_timeline_id {
            if timeline_id != event.timeline_id || batch_term != Some(event.term) {
                let _ = response_tx
                    .send(Err(Status::invalid_argument(
                        "record batch must contain one timeline and term",
                    )))
                    .await;
                return;
            }
        } else {
            batch_timeline_id = Some(event.timeline_id);
            batch_term = Some(event.term);
        }

        if let Err(current_term) = context.storage.check_term(event.timeline_id, event.term) {
            let _ = response_tx
                .send(Ok(RecordEventsResponse {
                    code: StatusCode::InvalidTerm.into(),
                    commit_offset: -1,
                    timeline_id: event.timeline_id,
                    term: current_term,
                }))
                .await;
            return;
        }

        writes.push((event, item.trunc));
    }

    let timeline_id = batch_timeline_id.unwrap_or_default();
    let term = batch_term.unwrap_or_default();
    let ack = Arc::new(BatchAck::new(response_tx, timeline_id, term, item_count));

    for (event, trunc) in writes {
        let permit = tokio::select! {
            _ = context.context.cancelled() => {
                ack.fail_status(Status::cancelled("record stream cancelled")).await;
                return;
            }
            permit = inflight_tx.reserve() => match permit {
                Ok(permit) => permit,
                Err(_) => {
                    ack.fail_status(Status::unavailable("record stream closed")).await;
                    return;
                }
            },
        };

        let encoded = event.encode_to_vec();
        let wal_offset = tokio::select! {
            _ = context.context.cancelled() => {
                ack.fail_status(Status::cancelled("record stream cancelled")).await;
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

async fn sync_record_inflight(
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
        let timeline_id = write.event.timeline_id;
        let offset = write.event.offset;
        let trunc = write.trunc;
        tokio::select! {
            _ = context.cancelled() => {
                write.ack.fail_status(Status::cancelled("record stream cancelled")).await;
                return false;
            }
            _ = storage.apply_write(write.event, trunc) => {}
        }
        storage.update_lra(timeline_id, offset);
        write.ack.complete_ok(offset).await;
    }
    true
}

async fn fail_pending_and_close(
    pending: &mut VecDeque<InflightWrite>,
    inflight_rx: &mut mpsc::Receiver<InflightWrite>,
) {
    inflight_rx.close();
    let status = Status::cancelled("record stream cancelled");
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
struct FetchStreamContext {
    storage: Arc<dyn Storage>,
    context: CancellationToken,
}

async fn run_fetch_stream<S>(
    mut stream: S,
    response_tx: mpsc::Sender<Result<FetchEventsResponse, Status>>,
    context: FetchStreamContext,
) where
    S: Stream<Item = Result<FetchEventsRequest, Status>> + Send + Unpin + 'static,
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
                    handle_fetch_request(request, response_tx.clone(), context.clone()).await
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

async fn handle_fetch_request(
    request: FetchEventsRequest,
    response_tx: mpsc::Sender<Result<FetchEventsResponse, Status>>,
    context: FetchStreamContext,
) -> Result<(), Status> {
    if request.end_offset < request.start_offset {
        return Err(Status::invalid_argument(
            "fetch end_offset must be greater than or equal to start_offset",
        ));
    }

    let events = context
        .storage
        .read_events(
            request.timeline_id,
            request.start_offset,
            request.end_offset,
        )
        .await
        .map_err(|error| Status::internal(error.to_string()))?;

    if events.is_empty() {
        send_fetch_response(
            &response_tx,
            FetchEventsResponse {
                code: StatusCode::Ok.into(),
                r#type: ChunkType::Full.into(),
                timeline_id: request.timeline_id,
                event: Vec::new(),
                advanced_offset: request.start_offset,
            },
            &context.context,
        )
        .await?;
        return Ok(());
    }

    let chunk_count = events.len().div_ceil(FETCH_CHUNK_SIZE);
    for (index, chunk) in events.chunks(FETCH_CHUNK_SIZE).enumerate() {
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

        send_fetch_response(
            &response_tx,
            FetchEventsResponse {
                code: StatusCode::Ok.into(),
                r#type: chunk_type.into(),
                timeline_id: request.timeline_id,
                event: chunk.to_vec(),
                advanced_offset,
            },
            &context.context,
        )
        .await?;
    }

    Ok(())
}

async fn send_fetch_response(
    response_tx: &mpsc::Sender<Result<FetchEventsResponse, Status>>,
    response: FetchEventsResponse,
    context: &CancellationToken,
) -> Result<(), Status> {
    tokio::select! {
        _ = context.cancelled() => Err(Status::cancelled("fetch stream cancelled")),
        result = response_tx.send(Ok(response)) => result
            .map_err(|_| Status::cancelled("fetch response stream closed")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::wal::WalOptions;
    use crate::storage::{Storage, UnitStorage};
    use futures_util::StreamExt;
    use lyra_proto::pb_ext::{Event, RecordEventsRequestItem};
    use tempfile::tempdir;

    fn event(timeline_id: i64, offset: i64, payload: &[u8]) -> Event {
        Event {
            timeline_id,
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

    async fn collect_fetch(
        storage: Arc<dyn Storage>,
        requests: Vec<FetchEventsRequest>,
    ) -> Vec<Result<FetchEventsResponse, Status>> {
        let (tx, rx) = mpsc::channel(RESPONSE_BUFFER);
        let context = FetchStreamContext {
            storage,
            context: CancellationToken::new(),
        };
        let request_stream = tokio_stream::iter(requests.into_iter().map(Ok));
        run_fetch_stream(request_stream, tx, context).await;
        ReceiverStream::new(rx).collect().await
    }

    #[tokio::test]
    async fn fetch_returns_events_in_requested_range() {
        let storage = test_storage().await;
        storage.apply_write(event(7, 1, b"a"), false).await;
        storage.apply_write(event(7, 2, b"b"), false).await;
        storage.apply_write(event(7, 3, b"c"), false).await;
        storage.apply_write(event(8, 1, b"other"), false).await;

        let responses = collect_fetch(
            storage,
            vec![FetchEventsRequest {
                timeline_id: 7,
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
    async fn fetch_empty_range_returns_full_empty_response() {
        let storage = test_storage().await;

        let responses = collect_fetch(
            storage,
            vec![FetchEventsRequest {
                timeline_id: 7,
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
    async fn fetch_rejects_invalid_range() {
        let storage = test_storage().await;

        let responses = collect_fetch(
            storage,
            vec![FetchEventsRequest {
                timeline_id: 7,
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
    async fn record_then_fetch_round_trips_through_unit_streams() {
        let storage = test_storage().await;
        let (record_tx, record_rx) = mpsc::channel(RESPONSE_BUFFER);
        let record_cancel = CancellationToken::new();
        let record_context = RecordStreamContext {
            storage: storage.clone(),
            context: record_cancel.clone(),
            inflight_capacity: 16,
        };
        let record_request = RecordEventsRequest {
            items: vec![RecordEventsRequestItem {
                event: Some(event(7, 1, b"a")),
                trunc: false,
                lra: 0,
            }],
        };
        let record_stream = tokio_stream::iter(vec![Ok(record_request)]);

        let record_handle =
            tokio::spawn(run_record_stream(record_stream, record_tx, record_context));

        let mut record_responses = ReceiverStream::new(record_rx);
        let record_response =
            tokio::time::timeout(std::time::Duration::from_secs(5), record_responses.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        assert_eq!(record_response.code, StatusCode::Ok as i32);
        assert_eq!(record_response.commit_offset, 1);
        record_cancel.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(5), record_handle)
            .await
            .unwrap()
            .unwrap();

        let fetch_responses = collect_fetch(
            storage,
            vec![FetchEventsRequest {
                timeline_id: 7,
                start_offset: 1,
                end_offset: 2,
            }],
        )
        .await;

        assert_eq!(fetch_responses.len(), 1);
        let fetch_response = fetch_responses.into_iter().next().unwrap().unwrap();
        assert_eq!(fetch_response.event.len(), 1);
        assert_eq!(fetch_response.event[0].offset, 1);
        assert_eq!(fetch_response.event[0].payload.as_deref(), Some(&b"a"[..]));
    }
}
