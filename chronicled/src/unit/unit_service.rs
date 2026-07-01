use crate::storage::write_cache::WriteCache;
use crate::unit::timeline_state::TimelineStateManager;
use crate::wal::wal::Wal;
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
pub struct UnitServiceTasks {
    context: CancellationToken,
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl UnitServiceTasks {
    pub fn new(context: CancellationToken) -> Self {
        Self {
            context,
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn context(&self) -> CancellationToken {
        self.context.clone()
    }

    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(future);
        self.handles.lock().unwrap().push(handle);
    }

    pub async fn shutdown(&self) {
        self.context.cancel();
        loop {
            let handles = {
                let mut handles = self.handles.lock().unwrap();
                if handles.is_empty() {
                    break;
                }
                std::mem::take(&mut *handles)
            };
            for handle in handles {
                if let Err(err) = handle.await {
                    warn!(error = ?err, "unit service stream task join error");
                }
            }
        }
    }
}

pub struct UnitService {
    wal: Wal,
    write_cache: WriteCache,
    timeline_state: Arc<TimelineStateManager>,
    tasks: UnitServiceTasks,
    inflight_capacity: usize,
}

pub struct UnitServiceConfig {
    pub wal: Wal,
    pub write_cache: WriteCache,
    pub timeline_state: Arc<TimelineStateManager>,
    pub tasks: UnitServiceTasks,
    pub inflight_capacity: usize,
}

impl UnitService {
    pub fn new(config: UnitServiceConfig) -> Self {
        Self {
            wal: config.wal,
            write_cache: config.write_cache,
            timeline_state: config.timeline_state,
            tasks: config.tasks,
            inflight_capacity: config.inflight_capacity,
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
        let context = self.tasks.context();
        let stream_context = RecordStreamContext {
            wal: self.wal.clone(),
            write_cache: self.write_cache.clone(),
            timeline_state: self.timeline_state.clone(),
            context,
            inflight_capacity: self.inflight_capacity,
        };
        self.tasks
            .spawn(run_record_stream(request.into_inner(), tx, stream_context));

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
        match self.timeline_state.fence(req.timeline_id, req.term) {
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
    wal: Wal,
    write_cache: WriteCache,
    timeline_state: Arc<TimelineStateManager>,
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
    let sync_loop = sync_record_inflight(
        inflight_rx,
        context.wal,
        context.write_cache,
        context.timeline_state,
        context.context,
    );
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

        if let Err(current_term) = context
            .timeline_state
            .check_term(event.timeline_id, event.term)
        {
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
            result = context.wal.append(encoded) => match result {
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
    wal: Wal,
    write_cache: WriteCache,
    timeline_state: Arc<TimelineStateManager>,
    context: CancellationToken,
) {
    let mut synced_offset = *wal.watch_synced().borrow();
    let mut watch = wal.watch_synced();
    let mut pending = VecDeque::new();
    let mut inflight_closed = false;

    loop {
        if !drain_synced_writes(
            &mut pending,
            synced_offset,
            &write_cache,
            &timeline_state,
            &context,
        )
        .await
        {
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
    write_cache: &WriteCache,
    timeline_state: &TimelineStateManager,
    context: &CancellationToken,
) -> bool {
    while pending
        .front()
        .is_some_and(|write| write.wal_offset <= synced_offset)
    {
        let write = pending.pop_front().unwrap();
        let timeline_id = write.event.timeline_id;
        let offset = write.event.offset;
        tokio::select! {
            _ = context.cancelled() => {
                write.ack.fail_status(Status::cancelled("record stream cancelled")).await;
                return false;
            }
            _ = write_cache.put(write.event, write.trunc) => {}
        }
        timeline_state.update_lra(timeline_id, offset);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::option::unit_options::IoMode;
    use chronicle_proto::pb_ext::{Event, RecordEventsRequestItem};
    use futures_util::StreamExt;

    fn test_event(term: i64, offset: i64, payload: &[u8]) -> Event {
        Event {
            timeline_id: 1,
            term,
            offset,
            payload: Some(payload.to_vec().into()),
            crc32: None,
            timestamp: offset * 100,
            schema_id: 0,
        }
    }

    async fn test_wal(dir: &std::path::Path) -> Wal {
        Wal::new(crate::wal::wal::WalOptions {
            dir: dir.to_string_lossy().to_string(),
            max_segment_size: None,
            io_mode: IoMode::Basic,
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn record_stream_syncs_to_wal_before_ack_and_cache_visibility() {
        let dir = tempfile::tempdir().unwrap();
        let wal = test_wal(&dir.path().join("wal")).await;
        let write_cache = WriteCache::new(1024 * 1024);
        let timeline_state = Arc::new(TimelineStateManager::new());
        timeline_state.fence(1, 1).unwrap();
        let context = CancellationToken::new();
        let (response_tx, mut response_rx) = mpsc::channel(4);
        let stream_context = RecordStreamContext {
            wal: wal.clone(),
            write_cache: write_cache.clone(),
            timeline_state: timeline_state.clone(),
            context,
            inflight_capacity: 8,
        };

        let event = test_event(1, 7, b"hello");
        let request = RecordEventsRequest {
            items: vec![RecordEventsRequestItem {
                event: Some(event.clone()),
                trunc: false,
                lra: -1,
            }],
        };

        run_record_stream(
            tokio_stream::iter(vec![Ok(request)]),
            response_tx,
            stream_context,
        )
        .await;

        let response = response_rx.recv().await.unwrap().unwrap();
        assert_eq!(response.code, StatusCode::Ok as i32);
        assert_eq!(response.commit_offset, 7);

        let cached = write_cache.scan(1, 7, 7);
        assert_eq!(cached, vec![event.clone()]);
        assert_eq!(timeline_state.get_state(1).unwrap().lra, 7);

        let mut replay = wal.read_stream().await;
        let replayed = replay.next().await.unwrap().unwrap();
        assert_eq!(Event::decode(replayed.as_slice()).unwrap(), event);
        assert!(replay.next().await.is_none());

        wal.shutdown().await;
    }

    #[tokio::test]
    async fn stale_term_write_is_rejected_before_wal_append() {
        let dir = tempfile::tempdir().unwrap();
        let wal = test_wal(&dir.path().join("wal")).await;
        let write_cache = WriteCache::new(1024 * 1024);
        let timeline_state = Arc::new(TimelineStateManager::new());
        timeline_state.fence(1, 2).unwrap();
        let context = CancellationToken::new();
        let (response_tx, mut response_rx) = mpsc::channel(4);
        let stream_context = RecordStreamContext {
            wal: wal.clone(),
            write_cache: write_cache.clone(),
            timeline_state: timeline_state.clone(),
            context,
            inflight_capacity: 8,
        };

        let request = RecordEventsRequest {
            items: vec![RecordEventsRequestItem {
                event: Some(test_event(1, 7, b"stale")),
                trunc: false,
                lra: -1,
            }],
        };

        run_record_stream(
            tokio_stream::iter(vec![Ok(request)]),
            response_tx,
            stream_context,
        )
        .await;

        let response = response_rx.recv().await.unwrap().unwrap();
        assert_eq!(response.code, StatusCode::InvalidTerm as i32);
        assert_eq!(response.commit_offset, -1);
        assert!(write_cache.scan(1, 0, 10).is_empty());

        let mut replay = wal.read_stream().await;
        assert!(replay.next().await.is_none());

        wal.shutdown().await;
    }

    #[tokio::test]
    async fn synced_write_blocked_on_cache_exits_on_cancellation() {
        let write_cache = WriteCache::new(1);
        write_cache.put_direct(test_event(1, 1, b"first"), false);
        assert!(write_cache.try_seal());
        write_cache.put_direct(test_event(1, 2, b"second"), false);

        let timeline_state = TimelineStateManager::new();
        let context = CancellationToken::new();
        let (response_tx, mut response_rx) = mpsc::channel(1);
        let ack = Arc::new(BatchAck::new(response_tx, 1, 1, 1));
        let mut pending = VecDeque::from([InflightWrite {
            wal_offset: 10,
            event: test_event(1, 3, b"blocked"),
            trunc: false,
            ack,
        }]);

        let cancel = context.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            cancel.cancel();
        });

        let completed = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            drain_synced_writes(&mut pending, 10, &write_cache, &timeline_state, &context),
        )
        .await
        .unwrap();

        assert!(!completed);
        assert!(pending.is_empty());
        let status = response_rx.recv().await.unwrap().unwrap_err();
        assert_eq!(status.code(), tonic::Code::Cancelled);
        assert!(write_cache.scan(1, 3, 3).is_empty());
    }
}
