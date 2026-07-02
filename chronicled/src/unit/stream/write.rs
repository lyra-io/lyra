use crate::storage::Storage;
use chronicle_proto::pb_ext::{RecordEventsRequest, RecordEventsResponse, StatusCode};
use futures_util::{Stream, StreamExt};
use prost::Message;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio_util::sync::CancellationToken;
use tonic::Status;

#[derive(Clone)]
pub(crate) struct RecordStreamContext {
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) context: CancellationToken,
    pub(crate) inflight_capacity: usize,
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

pub(crate) async fn run_record_stream<S>(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::option::unit_options::IoMode;
    use crate::storage::UnitStorage;
    use crate::storage::wal::WalOptions;
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

    async fn test_storage(dir: &std::path::Path) -> Arc<UnitStorage> {
        Arc::new(
            UnitStorage::open(WalOptions {
                dir: dir.to_string_lossy().to_string(),
                max_segment_size: None,
                io_mode: IoMode::Basic,
            })
            .await
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn record_stream_syncs_to_wal_before_ack_and_cache_visibility() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(&dir.path().join("wal")).await;
        storage.fence(1, 1).unwrap();
        let context = CancellationToken::new();
        let (response_tx, mut response_rx) = mpsc::channel(4);
        let stream_context = RecordStreamContext {
            storage: storage.clone(),
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

        let cached = storage.scan_cached(1, 7, 7);
        assert_eq!(cached, vec![event.clone()]);
        assert_eq!(storage.fence(1, 2).unwrap(), 7);

        let mut replay = storage.wal().read_stream();
        let replayed = replay.next().await.unwrap().unwrap();
        assert_eq!(Event::decode(replayed.as_slice()).unwrap(), event);
        assert!(replay.next().await.is_none());

        storage.shutdown().await;
    }

    #[tokio::test]
    async fn stale_term_write_is_rejected_before_wal_append() {
        let dir = tempfile::tempdir().unwrap();
        let storage = test_storage(&dir.path().join("wal")).await;
        storage.fence(1, 2).unwrap();
        let context = CancellationToken::new();
        let (response_tx, mut response_rx) = mpsc::channel(4);
        let stream_context = RecordStreamContext {
            storage: storage.clone(),
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
        assert!(storage.scan_cached(1, 0, 10).is_empty());

        let mut replay = storage.wal().read_stream();
        assert!(replay.next().await.is_none());

        storage.shutdown().await;
    }
}
