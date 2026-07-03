use super::ensemble::select_ensemble;
use crate::conn::conn_pool::ConnPool;
use crate::error::ChronicleError;
use crate::error_inner::InnerError;
use crate::{Event as UserEvent, Offset, TimelineOptions};
use backoff::future;
use chronicle_catalog::error::CatalogError;
use chronicle_catalog::{CatalogRef, Versioned};
use chronicle_proto::pb_catalog::{Segment, TimelineMeta, UnitInfo};
use chronicle_proto::pb_ext::{RecordEventsRequest, RecordEventsRequestItem};
use futures_util::future::{join_all, select_all};
use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const EMPTY_UNITS: VecDeque<UnitInfo> = VecDeque::new();

struct RecordRequest {
    event: UserEvent,
    reply: oneshot::Sender<Result<Offset, ChronicleError>>,
}

struct State {
    name: String,
    meta: TimelineMeta,
    catalog: CatalogRef,
    pool: Arc<ConnPool>,
    options: TimelineOptions,
    lrs: i64,
    needs_trunc: bool,
    ensemble: Vec<UnitInfo>,
    segment_start_offset: i64,
    segment_version: i64,
    wm_watches: Vec<(String, watch::Receiver<i64>)>,
}

struct InflightBatch {
    max_offset: i64,
    callbacks: Vec<(i64, oneshot::Sender<Result<Offset, ChronicleError>>)>,
}

struct StateMachineInner {
    timeline_id: i64,
    record_tx: mpsc::Sender<RecordRequest>,
    cancel: CancellationToken,
    task: sync::Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
pub(crate) struct StateMachine {
    inner: Arc<StateMachineInner>,
}

impl StateMachine {
    pub async fn start(
        name: &str,
        catalog: CatalogRef,
        pool: Arc<ConnPool>,
        options: &TimelineOptions,
    ) -> Result<Self, ChronicleError> {
        let mut state = State {
            name: name.to_string(),
            meta: TimelineMeta::default(),
            catalog,
            pool,
            options: options.clone(),
            lrs: 0,
            needs_trunc: false,
            ensemble: Vec::new(),
            segment_start_offset: 0,
            segment_version: -1,
            wm_watches: Vec::new(),
        };

        let max_batch_size = options.max_batch_size;
        let linger = options.linger;
        let cancel = CancellationToken::new();
        let (record_tx, record_rx) = mpsc::channel::<RecordRequest>(options.max_inflight);

        recover(&mut state).await?;
        let timeline_id = state.meta.timeline_id;

        let task = tokio::spawn(run(
            state,
            record_rx,
            cancel.clone(),
            max_batch_size,
            linger,
        ));

        Ok(Self {
            inner: Arc::new(StateMachineInner {
                timeline_id,
                record_tx,
                cancel,
                task: sync::Mutex::new(Some(task)),
            }),
        })
    }

    pub fn timeline_id(&self) -> i64 {
        self.inner.timeline_id
    }

    pub async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner
            .record_tx
            .send(RecordRequest {
                event,
                reply: reply_tx,
            })
            .await
            .map_err(|_| ChronicleError::Internal("state machine stopped".into()))?;
        reply_rx
            .await
            .map_err(|_| ChronicleError::Internal("state machine dropped".into()))?
    }

    pub async fn stop(&self) {
        self.inner.cancel.cancel();
        if let Some(task) = self.inner.task.lock().await.take() {
            let _ = task.await;
        }
    }
}

// ---------------------------------------------------------------------------
// Replicate — run an operation on all ensemble units with auto-recovery
//
// Returns the successful results once ALL units succeed.
// On partial failure: quarantine → find replacement → update vfs → retry.
// ---------------------------------------------------------------------------

async fn broadcast<R, Op, OpFut>(state: &mut State, op: Op) -> Vec<(UnitInfo, R)>
where
    R: Send + 'static,
    Op: Fn(Arc<ConnPool>, UnitInfo, Duration) -> OpFut,
    OpFut: Future<Output = (UnitInfo, Result<R, ChronicleError>)> + Send + 'static,
{
    let mut quarantined: VecDeque<UnitInfo> = VecDeque::new();

    loop {
        let futs = state.ensemble.iter().map(|unit| {
            op(
                state.pool.clone(),
                unit.clone(),
                state.options.request_timeout,
            )
        });
        let results = join_all(futs).await;

        let mut succeeded = Vec::new();
        let mut failed = Vec::new();
        for (unit, result) in results {
            match result {
                Ok(r) => succeeded.push((unit, r)),
                Err(e) => {
                    warn!(unit = ?unit, error = %e, "replicate: unit failed, quarantining");
                    failed.push(unit);
                }
            }
        }

        if failed.is_empty() {
            return succeeded;
        }

        for unit in failed {
            quarantined.push_back(unit);
        }

        let healthy: VecDeque<UnitInfo> = state
            .ensemble
            .iter()
            .filter(|u| !quarantined.contains(u))
            .cloned()
            .collect();

        let new_ensemble =
            find_replacement_ensemble(&state.catalog, &healthy, &mut quarantined).await;

        if let Err(e) = update_segment(state, &new_ensemble).await {
            warn!(error = %e, "failed to update vfs during recovery");
            continue;
        }

        state.ensemble = new_ensemble;

        let new_members: Vec<_> = state
            .ensemble
            .iter()
            .filter(|u| !healthy.contains(u))
            .cloned()
            .collect();
        if let Err(e) = fence_members(state, &new_members).await {
            warn!(error = %e, "fence new members failed, will retry");
            continue;
        }

        state.wm_watches = subscribe_watches(
            &state.ensemble,
            &state.pool,
            state.meta.timeline_id,
            state.meta.lra,
        );

        info!(
            timeline_id = state.meta.timeline_id,
            "ensemble replaced and fenced, retrying operation"
        );
    }
}

async fn find_replacement_ensemble(
    catalog: &CatalogRef,
    healthy: &VecDeque<UnitInfo>,
    quarantined: &mut VecDeque<UnitInfo>,
) -> Vec<UnitInfo> {
    let quarantined_mu = Mutex::new(std::mem::take(quarantined));
    let backoff = backoff::ExponentialBackoffBuilder::new()
        .with_max_elapsed_time(None)
        .build();
    let result = future::retry_notify(
        backoff,
        || async {
            loop {
                let candidates = catalog
                    .list_writable_units()
                    .await
                    .map_err(|e| backoff::Error::transient(InnerError::Catalog(e)))?;
                let q = quarantined_mu.lock().unwrap();
                match select_ensemble(&candidates, healthy, &q) {
                    Some(ensemble) => return Ok(ensemble),
                    None => {
                        if q.is_empty() {
                            return Err(backoff::Error::transient(InnerError::UnitNotEnough(
                                "not enough candidates for ensemble".into(),
                            )));
                        }
                        drop(q);
                        quarantined_mu.lock().unwrap().pop_front();
                        continue;
                    }
                }
            }
        },
        |_e, retry_in| {
            warn!(retry_in = ?retry_in, "not enough candidates, retrying");
        },
    )
    .await;
    *quarantined = quarantined_mu.into_inner().unwrap();
    result.unwrap_or_default()
}

async fn fence_members(state: &State, members: &[UnitInfo]) -> Result<(), ChronicleError> {
    let futs = members.iter().map(|unit| {
        let pool = state.pool.clone();
        let timeout = state.options.request_timeout;
        let timeline_id = state.meta.timeline_id;
        let term = state.meta.term;
        let unit = unit.clone();
        async move {
            let result = match pool.get_or_connect(&unit.address) {
                Ok(conn) => conn
                    .fence_with_retry(timeline_id, term, timeout)
                    .await
                    .map_err(|e| ChronicleError::Internal(e.to_string())),
                Err(e) => Err(ChronicleError::Transport(e.to_string())),
            };
            (unit, result)
        }
    });
    let results = join_all(futs).await;
    for (unit, result) in results {
        if let Err(e) = result {
            warn!(unit = ?unit, error = %e, "fence new member failed");
            return Err(e);
        }
    }
    Ok(())
}

fn subscribe_watches(
    ensemble: &[UnitInfo],
    pool: &ConnPool,
    timeline_id: i64,
    lra: i64,
) -> Vec<(String, watch::Receiver<i64>)> {
    let mut watches = Vec::new();
    for unit in ensemble {
        if let Ok(conn) = pool.get_or_connect(&unit.address) {
            let rx = conn.subscribe_watermark(timeline_id, lra);
            watches.push((unit.address.clone(), rx));
        }
    }
    watches
}

// ---------------------------------------------------------------------------
// Init — new term, vfs, fence via replicate
// ---------------------------------------------------------------------------

async fn recover(state: &mut State) -> Result<(), ChronicleError> {
    state.meta = state
        .catalog
        .tl_new_term(&state.name)
        .await
        .map_err(|e| ChronicleError::Internal(e.to_string()))?;

    let writable_segment = get_or_init_last_segment(
        state.catalog.clone(),
        &state.meta.name,
        state.options.replication_factor,
    )
    .await
    .map_err(|e| ChronicleError::UnitNotEnough(e.to_string()))?;

    state.ensemble = writable_segment.value.ensemble.clone();
    state.segment_start_offset = writable_segment.value.start_offset;
    state.segment_version = writable_segment.version;

    info!(
        timeline_id = state.meta.timeline_id,
        term = state.meta.term,
        "fencing ensemble"
    );

    let timeline_id = state.meta.timeline_id;
    let term = state.meta.term;
    let fence_results = broadcast(state, |pool, unit, timeout| async move {
        let result = match pool.get_or_connect(&unit.address) {
            Ok(conn) => conn
                .fence_with_retry(timeline_id, term, timeout)
                .await
                .map_err(|e| ChronicleError::Internal(e.to_string())),
            Err(e) => Err(ChronicleError::Transport(e.to_string())),
        };
        (unit, result)
    })
    .await;

    let min_lra = fence_results
        .iter()
        .map(|(_, resp)| resp.lra)
        .min()
        .unwrap_or(0);
    let max_lra = fence_results
        .iter()
        .map(|(_, resp)| resp.lra)
        .max()
        .unwrap_or(0);

    let lra = min_lra;
    state.needs_trunc = lra > 0 || min_lra != max_lra;

    if state.meta.lra != lra {
        state.meta.lra = lra;
        state.lrs = lra;
        state.meta = state
            .catalog
            .timeline_update(&state.meta, state.meta.version)
            .await
            .map_err(|e| ChronicleError::Internal(e.to_string()))?;
    }

    state.wm_watches = subscribe_watches(
        &state.ensemble,
        &state.pool,
        state.meta.timeline_id,
        state.meta.lra,
    );

    info!(
        timeline_id = state.meta.timeline_id,
        term = state.meta.term,
        lra = lra,
        "init complete"
    );

    Ok(())
}

async fn get_or_init_last_segment(
    catalog: CatalogRef,
    timeline_name: &str,
    replication_factor: usize,
) -> Result<Versioned<Segment>, CatalogError> {
    if let Some(last) = catalog.get_last_segment(timeline_name).await? {
        return Ok(last);
    }

    let units = catalog.list_writable_units().await?;
    let ensemble = select_ensemble(&units, &EMPTY_UNITS, &EMPTY_UNITS).ok_or_else(|| {
        CatalogError::NotFound(format!(
            "need {} writable units, have {}",
            replication_factor,
            units.len()
        ))
    })?;
    let segment = Segment {
        ensemble,
        start_offset: 1,
    };

    match catalog.put_segment(timeline_name, &segment, -1).await {
        Ok(segment) => Ok(segment),
        Err(CatalogError::VersionConflict { .. }) => catalog
            .get_last_segment(timeline_name)
            .await?
            .ok_or_else(|| CatalogError::Internal("vfs vanished after conflict".into())),
        Err(error) => Err(error),
    }
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

async fn run(
    mut state: State,
    mut record_rx: mpsc::Receiver<RecordRequest>,
    cancel: CancellationToken,
    max_batch_size: usize,
    linger: Duration,
) {
    let mut batch: Vec<RecordRequest> = Vec::with_capacity(max_batch_size);
    let mut inflight: VecDeque<InflightBatch> = VecDeque::new();
    let mut linger_tick = tokio::time::interval(linger);

    loop {
        tokio::select! {
            biased;

            _ = cancel.cancelled() => {
                for batch in inflight.drain(..) {
                    for (_, tx) in batch.callbacks {
                        let _ = tx.send(Err(ChronicleError::Canceled));
                    }
                }
                for req in batch.drain(..) {
                    let _ = req.reply.send(Err(ChronicleError::Canceled));
                }
                return;
            }

            lra = wait_watermark_advance(&mut state.wm_watches) => {
                handle_watermark_changed(&mut state, &mut inflight, lra);
            }

            count = record_rx.recv_many(&mut batch, max_batch_size) => {
                if count == 0 {
                    continue;
                }
                if batch.len() >= max_batch_size {
                    flush_batch(&mut state, &mut batch, &mut inflight).await;
                }
            }

            _ = linger_tick.tick() => {
                if !batch.is_empty() {
                    flush_batch(&mut state, &mut batch, &mut inflight).await;
                }
            }
        }
    }
}

async fn wait_watermark_advance(watches: &mut [(String, watch::Receiver<i64>)]) -> i64 {
    if watches.is_empty() {
        return std::future::pending().await;
    }
    let futs: Vec<_> = watches
        .iter_mut()
        .map(|(_, rx)| Box::pin(rx.changed()))
        .collect();
    let _ = select_all(futs).await;
    watches
        .iter()
        .map(|(_, rx)| *rx.borrow())
        .min()
        .unwrap_or(-1)
}

fn handle_watermark_changed(state: &mut State, inflight: &mut VecDeque<InflightBatch>, lra: i64) {
    while let Some(front) = inflight.front() {
        if front.max_offset > lra {
            break;
        }
        let batch = inflight.pop_front().unwrap();
        for (offset, tx) in batch.callbacks {
            if tx.send(Ok(Offset(offset))).is_err() {
                warn!(offset = offset, "record callback failed");
            }
        }
    }
    state.meta.lra = lra;
}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

fn prepare_batch(
    state: &mut State,
    events: Vec<UserEvent>,
) -> (Vec<RecordEventsRequestItem>, Vec<i64>) {
    let mut items = Vec::with_capacity(events.len());
    let mut offsets = Vec::with_capacity(events.len());

    for event in events {
        let offset = state.lrs + 1;
        state.lrs = offset;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let proto_event = chronicle_proto::pb_ext::Event {
            timeline_id: state.meta.timeline_id,
            term: state.meta.term,
            offset,
            payload: Some(event.payload.into()),
            crc32: None,
            timestamp: now,
            schema_id: 0,
        };

        let item = RecordEventsRequestItem {
            event: Some(proto_event),
            trunc: state.needs_trunc,
            lra: state.meta.lra,
        };
        state.needs_trunc = false;

        items.push(item);
        offsets.push(offset);
    }

    (items, offsets)
}

async fn flush_batch(
    state: &mut State,
    batch: &mut Vec<RecordRequest>,
    inflight: &mut VecDeque<InflightBatch>,
) {
    let mut events = Vec::with_capacity(batch.len());
    let mut replies = Vec::with_capacity(batch.len());
    for req in batch.drain(..) {
        events.push(req.event);
        replies.push(req.reply);
    }

    let (items, offsets) = prepare_batch(state, events);
    let max_offset = *offsets.last().unwrap();
    let request = RecordEventsRequest { items };

    broadcast(state, |pool, unit, timeout| {
        let request = request.clone();
        async move {
            let result = match pool.get_or_connect(&unit.address) {
                Ok(conn) => conn.send_record_with_retry(request, timeout).await,
                Err(e) => Err(ChronicleError::Transport(e.to_string())),
            };
            (unit, result)
        }
    })
    .await;

    let callbacks: Vec<_> = offsets.into_iter().zip(replies).collect();
    inflight.push_back(InflightBatch {
        max_offset,
        callbacks,
    });
}

// ---------------------------------------------------------------------------
// Segment management
// ---------------------------------------------------------------------------

async fn update_segment(
    state: &mut State,
    new_ensemble: &[UnitInfo],
) -> Result<(), ChronicleError> {
    let has_records = state.lrs >= state.segment_start_offset;
    let segment = Segment {
        ensemble: new_ensemble.to_vec(),
        start_offset: if has_records {
            state.meta.lra + 1
        } else {
            state.segment_start_offset
        },
    };
    let expected_version = if has_records {
        -1
    } else {
        state.segment_version
    };

    let versioned = state
        .catalog
        .put_segment(&state.name, &segment, expected_version)
        .await
        .map_err(|e| ChronicleError::Internal(e.to_string()))?;

    state.segment_start_offset = segment.start_offset;
    state.segment_version = versioned.version;
    Ok(())
}
