use super::recoverable_stream::RecoverableStream;
use crate::error::ChronicleError;
use crate::error_inner::InnerError;
use backoff::future;
use chronicle_proto::pb_ext::{
    FenceRequest, FenceResponse, FetchEventsRequest, FetchEventsResponse, RecordEventsRequest,
    RecordEventsResponse, StatusCode, chronicle_client::ChronicleClient,
};
use dashmap::DashMap;
use futures_util::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::warn;
// ---------------------------------------------------------------------------
// ConnOptions
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ConnOptions {
    pub conns_per_unit: usize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub keep_alive_interval: Duration,
    pub keep_alive_timeout: Duration,
}

impl Default for ConnOptions {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            conns_per_unit: cpus.max(1),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            keep_alive_interval: Duration::from_secs(10),
            keep_alive_timeout: Duration::from_secs(5),
        }
    }
}

// ---------------------------------------------------------------------------
// Conn — one logical connection with its own record and fetch streams
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Conn {
    endpoint: String,
    client: ChronicleClient<Channel>,
    record_stream: RecoverableStream<RecordEventsRequest>,
    fetch_stream: RecoverableStream<FetchEventsRequest>,
    wm_subscribers: Arc<DashMap<i64, watch::Sender<i64>>>,
    fetch_subscribers: Arc<DashMap<i64, mpsc::Sender<Result<FetchEventsResponse, ChronicleError>>>>,
}

impl Conn {
    pub(crate) fn new(endpoint: String, client: ChronicleClient<Channel>) -> Self {
        let wm_subscribers = Arc::new(DashMap::new());
        let fetch_subscribers: Arc<
            DashMap<i64, mpsc::Sender<Result<FetchEventsResponse, ChronicleError>>>,
        > = Arc::new(DashMap::new());

        let record_stream = {
            let client = client.clone();
            let ep = endpoint.clone();
            let subs = wm_subscribers.clone();
            RecoverableStream::new(Arc::new(move || {
                let mut client = client.clone();
                let ep = ep.clone();
                let subs = subs.clone();
                Box::pin(async move {
                    let (tx, rx) = mpsc::channel::<RecordEventsRequest>(64);
                    let stream = ReceiverStream::new(rx);
                    let response = client
                        .record(stream)
                        .await
                        .map_err(|e| ChronicleError::Transport(e.to_string()))?;
                    let handle =
                        tokio::spawn(record_response_reader(ep, response.into_inner(), subs));
                    Ok((tx, handle))
                })
            }))
        };

        let fetch_stream = {
            let client = client.clone();
            let ep = endpoint.clone();
            let subs = fetch_subscribers.clone();
            RecoverableStream::new(Arc::new(move || {
                let mut client = client.clone();
                let ep = ep.clone();
                let subs = subs.clone();
                Box::pin(async move {
                    let (tx, rx) = mpsc::channel::<FetchEventsRequest>(64);
                    let stream = ReceiverStream::new(rx);
                    let response = client
                        .fetch(stream)
                        .await
                        .map_err(|e| ChronicleError::Transport(e.to_string()))?;
                    let handle =
                        tokio::spawn(fetch_response_reader(ep, response.into_inner(), subs));
                    Ok((tx, handle))
                })
            }))
        };

        Self {
            endpoint,
            client,
            record_stream,
            fetch_stream,
            wm_subscribers,
            fetch_subscribers,
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    // -- watermark subscribers ------------------------------------------------

    pub fn subscribe_watermark(&self, timeline_id: i64, initial: i64) -> watch::Receiver<i64> {
        if let Some(existing) = self.wm_subscribers.get(&timeline_id) {
            return existing.subscribe();
        }
        let (tx, rx) = watch::channel(initial);
        self.wm_subscribers.insert(timeline_id, tx);
        rx
    }

    pub fn unsubscribe_watermark(&self, timeline_id: i64) {
        self.wm_subscribers.remove(&timeline_id);
    }

    // -- lifecycle ------------------------------------------------------------

    /// Gracefully close both record and fetch streams: drop request senders
    /// so servers see end-of-ss, wait for response readers to drain,
    /// then clear all subscribers.
    pub async fn close(&self) {
        self.record_stream.close().await;
        self.fetch_stream.close().await;
        self.wm_subscribers.clear();
        self.fetch_subscribers.clear();
    }

    // -- RPC ------------------------------------------------------------------

    pub async fn fence(&self, timeline_id: i64, term: i64) -> Result<FenceResponse, InnerError> {
        let mut client = self.client.clone();
        let response = client
            .fence(FenceRequest { timeline_id, term })
            .await
            .map_err(InnerError::from)?;
        Ok(response.into_inner())
    }

    /// Send a record request. The gRPC ss is lazily opened on first call
    /// and automatically reconnected if the previous ss died.
    pub async fn send_record(&self, request: RecordEventsRequest) -> Result<(), ChronicleError> {
        self.record_stream.send(request).await
    }

    pub async fn fence_with_retry(
        &self,
        timeline_id: i64,
        term: i64,
        timeout: Duration,
    ) -> Result<FenceResponse, InnerError> {
        let backoff = backoff::ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(timeout))
            .build();
        future::retry_notify(
            backoff,
            || async {
                match self.fence(timeline_id, term).await {
                    Ok(resp) => Ok(resp),
                    Err(e @ InnerError::InvalidTerm { .. }) => Err(backoff::Error::permanent(e)),
                    Err(e) => Err(backoff::Error::transient(e)),
                }
            },
            |e, retry_in| {
                warn!(
                    endpoint = %self.endpoint,
                    error = %e,
                    retry_in = ?retry_in,
                    "fence failed, retrying"
                );
            },
        )
        .await
    }

    pub async fn send_record_with_retry(
        &self,
        request: RecordEventsRequest,
        timeout: Duration,
    ) -> Result<(), ChronicleError> {
        let backoff = backoff::ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(timeout))
            .build();
        future::retry_notify(
            backoff,
            || async {
                self.send_record(request.clone())
                    .await
                    .map_err(backoff::Error::transient)
            },
            |e, retry_in| {
                warn!(
                    endpoint = %self.endpoint,
                    error = %e,
                    retry_in = ?retry_in,
                    "send_record failed, retrying"
                );
            },
        )
        .await
    }

    /// Start a fetch. Subscribes for responses, sends the request through the
    /// shared fetch ss, and returns a [`FetchStream`] that yields
    /// responses. Automatically unsubscribes when dropped.
    pub async fn fetch(&self, request: FetchEventsRequest) -> Result<FetchStream, ChronicleError> {
        let timeline_id = request.timeline_id;
        let (tx, rx) = mpsc::channel::<Result<FetchEventsResponse, ChronicleError>>(64);
        let entry = self.fetch_subscribers.entry(timeline_id);
        self.fetch_stream.send(request).await?;
        entry.insert(tx);
        Ok(FetchStream {
            rx,
            timeline_id,
            subscribers: self.fetch_subscribers.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// FetchStream — Stream wrapper that unsubscribes on drop
// ---------------------------------------------------------------------------

pub struct FetchStream {
    rx: mpsc::Receiver<Result<FetchEventsResponse, ChronicleError>>,
    timeline_id: i64,
    subscribers: Arc<DashMap<i64, mpsc::Sender<Result<FetchEventsResponse, ChronicleError>>>>,
}

impl Stream for FetchStream {
    type Item = Result<FetchEventsResponse, ChronicleError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl Drop for FetchStream {
    fn drop(&mut self) {
        self.subscribers.remove(&self.timeline_id);
    }
}

// ---------------------------------------------------------------------------
// Record response reader — demuxes watermarks by timeline_id to subscribers
// ---------------------------------------------------------------------------

async fn record_response_reader(
    endpoint: String,
    mut stream: tonic::Streaming<RecordEventsResponse>,
    subscribers: Arc<DashMap<i64, watch::Sender<i64>>>,
) {
    let reason = loop {
        match stream.message().await {
            Ok(Some(resp)) => {
                if resp.code == StatusCode::Ok as i32 {
                    if let Some(tx) = subscribers.get(&resp.timeline_id) {
                        let _ = tx.send(resp.commit_offset);
                    }
                } else {
                    warn!(
                        endpoint = %endpoint,
                        timeline_id = resp.timeline_id,
                        code = resp.code,
                        "record_response_reader: non-ok response"
                    );
                }
            }
            Ok(None) => break "ss ended".to_string(),
            Err(e) => {
                warn!(endpoint = %endpoint, error = %e, "record_response_reader: error");
                break e.to_string();
            }
        }
    };
    warn!(endpoint = %endpoint, reason = %reason, "record_response_reader: ended");
}

// ---------------------------------------------------------------------------
// Fetch response reader — demuxes fetch responses by timeline_id to subscribers
// ---------------------------------------------------------------------------

async fn fetch_response_reader(
    endpoint: String,
    mut stream: tonic::Streaming<FetchEventsResponse>,
    subscribers: Arc<DashMap<i64, mpsc::Sender<Result<FetchEventsResponse, ChronicleError>>>>,
) {
    let reason = loop {
        match stream.message().await {
            Ok(Some(resp)) => {
                let timeline_id = resp.timeline_id;
                if let Some(tx) = subscribers.get(&timeline_id) {
                    match tx.try_send(Ok(resp)) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            subscribers.remove(&timeline_id);
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!(
                                endpoint = %endpoint,
                                timeline_id = timeline_id,
                                "fetch subscriber full, dropping response"
                            );
                        }
                    }
                }
            }
            Ok(None) => break "ss ended".to_string(),
            Err(e) => {
                warn!(endpoint = %endpoint, error = %e, "fetch_response_reader: error");
                break e.to_string();
            }
        }
    };
    // Notify all subscribers with the error, then clear.
    for entry in subscribers.iter() {
        let _ = entry
            .value()
            .try_send(Err(ChronicleError::Transport(reason.clone())));
    }
    subscribers.clear();
    warn!(endpoint = %endpoint, reason = %reason, "fetch_response_reader: ended");
}
