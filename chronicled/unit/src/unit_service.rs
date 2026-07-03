use crate::storage::Storage;
use chronicle_proto::pb_ext::chronicle_server::Chronicle;
use chronicle_proto::pb_ext::{
    FenceRequest, FenceResponse, FetchEventsRequest, FetchEventsResponse, RecordEventsRequest,
    RecordEventsResponse, StatusCode,
};
use futures_util::{Stream, StreamExt};
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
}

#[tonic::async_trait]
impl Chronicle for UnitService {
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

#[derive(Clone)]
struct RecordStreamContext {
    storage: Arc<dyn Storage>,
    context: CancellationToken,
    inflight_capacity: usize,
}

struct InflightWrite {
    wal_offset: i64,
    event: chronicle_proto::pb_ext::Event,
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
    let receive_loop =
        receive_record_requests(stream, response_tx.clone(), inflight_tx, context.clone());
    let sync_loop = sync_record_inflight(inflight_rx, context.storage, context.context);
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
) {
    let mut synced_offset = *storage.watch_synced().borrow();
    let mut watch = storage.watch_synced();
    let mut pending = VecDeque::new();
    let mut inflight_closed = false;

    loop {
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
