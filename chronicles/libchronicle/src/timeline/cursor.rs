use crate::Event;
use crate::conn::Conn;
use crate::conn::conn_pool::ConnPool;
use crate::error::ChronicleError;
use chronicle_catalog::{Catalog, CatalogRef};
use chronicle_proto::pb_catalog::Segment;
use chronicle_proto::pb_ext::{ChunkType, FetchEventsRequest};
use futures_util::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tracing::warn;

type InnerStream = Pin<Box<dyn Stream<Item = Result<Event, ChronicleError>> + Send>>;

const MAX_RETRIES: usize = 5;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);

pub struct EventStream {
    timeline_id: i64,
    timeline_name: String,
    catalog: CatalogRef,
    pool: Arc<ConnPool>,
    position: Arc<AtomicI64>,
    inner: Option<InnerStream>,
    tail: bool,
    limit: Option<usize>,
    yielded: usize,
    timeout: Option<Duration>,
    started_at: Instant,
    poll_interval: Duration,
    current_backoff: Duration,
    retries: usize,
}

impl EventStream {
    pub(crate) fn new(
        timeline_id: i64,
        timeline_name: String,
        catalog: CatalogRef,
        pool: Arc<ConnPool>,
        start_offset: i64,
    ) -> Self {
        Self {
            timeline_id,
            timeline_name,
            catalog,
            pool,
            position: Arc::new(AtomicI64::new(start_offset)),
            inner: None,
            tail: false,
            limit: None,
            yielded: 0,
            timeout: None,
            started_at: Instant::now(),
            poll_interval: DEFAULT_POLL_INTERVAL,
            current_backoff: DEFAULT_POLL_INTERVAL,
            retries: 0,
        }
    }

    pub(crate) fn with_tail(mut self) -> Self {
        self.tail = true;
        self
    }

    pub(crate) fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub(crate) fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    fn pick_conn(pool: &ConnPool, segment: &Segment) -> Result<Conn, ChronicleError> {
        for unit in &segment.ensemble {
            match pool.get_or_connect(&unit.address) {
                Ok(conn) => return Ok(conn),
                Err(_) => continue,
            }
        }
        Err(ChronicleError::UnitNotEnough(
            "no reachable unit in vfs ensemble".into(),
        ))
    }

    async fn open_inner(
        timeline_id: i64,
        timeline_name: &str,
        catalog: &dyn Catalog,
        pool: &ConnPool,
        position: &Arc<AtomicI64>,
    ) -> Result<InnerStream, ChronicleError> {
        let start = position.load(Ordering::Relaxed);

        let segment = catalog
            .get_segment_for_offset(timeline_name, start)
            .await
            .map_err(|e| ChronicleError::Internal(format!("vfs lookup failed: {}", e)))?
            .ok_or_else(|| ChronicleError::Internal(format!("no vfs covers offset {}", start)))?;

        let conn = Self::pick_conn(pool, &segment.value)?;

        let mut fetch = conn
            .fetch(FetchEventsRequest {
                timeline_id,
                start_offset: start,
                end_offset: i64::MAX,
            })
            .await?;

        let position = position.clone();

        let stream = async_stream::try_stream! {
            while let Some(result) = fetch.next().await {
                let response = result?;
                let is_final = matches!(
                    response.r#type(),
                    ChunkType::Full | ChunkType::Last
                );
                for proto_event in response.event {
                    let evt = Event {
                        offset: Some(proto_event.offset),
                        timestamp: Some(proto_event.timestamp),
                        payload: proto_event.payload.map(|b| b.to_vec()).unwrap_or_default(),
                        key: None,
                        txn_id: None,
                    };
                    position.store(proto_event.offset + 1, Ordering::Relaxed);
                    yield evt;
                }
                if is_final {
                    break;
                }
            }
        };

        Ok(Box::pin(stream))
    }

    fn is_timed_out(&self) -> bool {
        if let Some(timeout) = self.timeout {
            self.started_at.elapsed() >= timeout
        } else {
            false
        }
    }

    fn is_limit_reached(&self) -> bool {
        if let Some(limit) = self.limit {
            self.yielded >= limit
        } else {
            false
        }
    }
}

impl Stream for EventStream {
    type Item = Result<Event, ChronicleError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.is_limit_reached() || self.is_timed_out() {
                return Poll::Ready(None);
            }

            if self.inner.is_none() {
                let timeline_id = self.timeline_id;
                let timeline_name = self.timeline_name.clone();
                let catalog = self.catalog.clone();
                let pool = self.pool.clone();
                let position = self.position.clone();

                let mut fut = Box::pin(async move {
                    Self::open_inner(
                        timeline_id,
                        &timeline_name,
                        catalog.as_ref(),
                        &pool,
                        &position,
                    )
                    .await
                });
                match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(stream)) => {
                        self.inner = Some(stream);
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            let stream = self.inner.as_mut().unwrap();
            match stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(event))) => {
                    self.retries = 0;
                    self.current_backoff = self.poll_interval;
                    self.yielded += 1;
                    return Poll::Ready(Some(Ok(event)));
                }
                Poll::Ready(Some(Err(e))) => {
                    self.retries += 1;
                    if self.retries > MAX_RETRIES {
                        return Poll::Ready(Some(Err(e)));
                    }
                    warn!(
                        error = %e,
                        retry = self.retries,
                        "fetch ss error, reconnecting"
                    );
                    self.inner = None;
                    continue;
                }
                Poll::Ready(None) => {
                    if self.tail {
                        // Re-poll — next iteration will lazy-load the vfs again.
                        self.inner = None;
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
