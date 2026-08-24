use super::recoverable_stream::RecoverableStream;
use crate::error::LyraError;
use crate::error_inner::InnerError;
use backoff::future;
use dashmap::DashMap;
use futures_util::Stream;
use meta::proto::pb_ext::{
    AppendEventsRequest, AppendEventsResponse, FenceRequest, FenceResponse, ReadEventsRequest,
    ReadEventsResponse, StatusCode, lyra_client::LyraClient,
};
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
// Conn — one logical connection with its own append and read streams
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Conn {
    endpoint: String,
    client: LyraClient<Channel>,
    append_stream: RecoverableStream<AppendEventsRequest>,
    read_stream: RecoverableStream<ReadEventsRequest>,
    wm_subscribers: Arc<DashMap<i64, watch::Sender<i64>>>,
    read_subscribers: Arc<DashMap<i64, mpsc::Sender<Result<ReadEventsResponse, LyraError>>>>,
}

impl Conn {
    pub(crate) fn new(endpoint: String, client: LyraClient<Channel>) -> Self {
        let wm_subscribers = Arc::new(DashMap::new());
        let read_subscribers: Arc<
            DashMap<i64, mpsc::Sender<Result<ReadEventsResponse, LyraError>>>,
        > = Arc::new(DashMap::new());

        let append_stream = {
            let client = client.clone();
            let ep = endpoint.clone();
            let subs = wm_subscribers.clone();
            RecoverableStream::new(Arc::new(move || {
                let mut client = client.clone();
                let ep = ep.clone();
                let subs = subs.clone();
                Box::pin(async move {
                    let (tx, rx) = mpsc::channel::<AppendEventsRequest>(64);
                    let stream = ReceiverStream::new(rx);
                    let response = client
                        .append(stream)
                        .await
                        .map_err(|e| LyraError::Transport(e.to_string()))?;
                    let handle =
                        tokio::spawn(append_response_reader(ep, response.into_inner(), subs));
                    Ok((tx, handle))
                })
            }))
        };

        let read_stream = {
            let client = client.clone();
            let ep = endpoint.clone();
            let subs = read_subscribers.clone();
            RecoverableStream::new(Arc::new(move || {
                let mut client = client.clone();
                let ep = ep.clone();
                let subs = subs.clone();
                Box::pin(async move {
                    let (tx, rx) = mpsc::channel::<ReadEventsRequest>(64);
                    let stream = ReceiverStream::new(rx);
                    let response = client
                        .read(stream)
                        .await
                        .map_err(|e| LyraError::Transport(e.to_string()))?;
                    let handle =
                        tokio::spawn(read_response_reader(ep, response.into_inner(), subs));
                    Ok((tx, handle))
                })
            }))
        };

        Self {
            endpoint,
            client,
            append_stream,
            read_stream,
            wm_subscribers,
            read_subscribers,
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    // -- watermark subscribers ------------------------------------------------

    pub fn subscribe_watermark(&self, stream_id: i64, initial: i64) -> watch::Receiver<i64> {
        if let Some(existing) = self.wm_subscribers.get(&stream_id) {
            return existing.subscribe();
        }
        let (tx, rx) = watch::channel(initial);
        self.wm_subscribers.insert(stream_id, tx);
        rx
    }

    pub fn unsubscribe_watermark(&self, stream_id: i64) {
        self.wm_subscribers.remove(&stream_id);
    }

    // -- lifecycle ------------------------------------------------------------

    /// Gracefully close both append and read streams: drop request senders
    /// so servers see end-of-ss, wait for response readers to drain,
    /// then clear all subscribers.
    pub async fn close(&self) {
        self.append_stream.close().await;
        self.read_stream.close().await;
        self.wm_subscribers.clear();
        self.read_subscribers.clear();
    }

    // -- RPC ------------------------------------------------------------------

    pub async fn fence(&self, stream_id: i64, term: i64) -> Result<FenceResponse, InnerError> {
        let mut client = self.client.clone();
        let response = client
            .fence(FenceRequest { stream_id, term })
            .await
            .map_err(InnerError::from)?;
        Ok(response.into_inner())
    }

    /// Send an append request. The gRPC ss is lazily opened on first call
    /// and automatically reconnected if the previous ss died.
    pub async fn send_append(&self, request: AppendEventsRequest) -> Result<(), LyraError> {
        self.append_stream.send(request).await
    }

    pub async fn fence_with_retry(
        &self,
        stream_id: i64,
        term: i64,
        timeout: Duration,
    ) -> Result<FenceResponse, InnerError> {
        let backoff = backoff::ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(timeout))
            .build();
        future::retry_notify(
            backoff,
            || async {
                match self.fence(stream_id, term).await {
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

    pub async fn send_append_with_retry(
        &self,
        request: AppendEventsRequest,
        timeout: Duration,
    ) -> Result<(), LyraError> {
        let backoff = backoff::ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(timeout))
            .build();
        future::retry_notify(
            backoff,
            || async {
                self.send_append(request.clone())
                    .await
                    .map_err(backoff::Error::transient)
            },
            |e, retry_in| {
                warn!(
                    endpoint = %self.endpoint,
                    error = %e,
                    retry_in = ?retry_in,
                    "send_append failed, retrying"
                );
            },
        )
        .await
    }

    /// Start a read. Subscribes for responses, sends the request through the
    /// shared read ss, and returns a [`ReadStream`] that yields
    /// responses. Automatically unsubscribes when dropped.
    pub async fn read(&self, request: ReadEventsRequest) -> Result<ReadStream, LyraError> {
        let stream_id = request.stream_id;
        let (tx, rx) = mpsc::channel::<Result<ReadEventsResponse, LyraError>>(64);
        self.read_subscribers.insert(stream_id, tx);
        if let Err(error) = self.read_stream.send(request).await {
            self.read_subscribers.remove(&stream_id);
            return Err(error);
        }
        Ok(ReadStream {
            rx,
            stream_id,
            subscribers: self.read_subscribers.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// ReadStream — Stream wrapper that unsubscribes on drop
// ---------------------------------------------------------------------------

pub struct ReadStream {
    rx: mpsc::Receiver<Result<ReadEventsResponse, LyraError>>,
    stream_id: i64,
    subscribers: Arc<DashMap<i64, mpsc::Sender<Result<ReadEventsResponse, LyraError>>>>,
}

impl Stream for ReadStream {
    type Item = Result<ReadEventsResponse, LyraError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl Drop for ReadStream {
    fn drop(&mut self) {
        self.subscribers.remove(&self.stream_id);
    }
}

// ---------------------------------------------------------------------------
// Append response reader — demuxes watermarks by stream_id to subscribers
// ---------------------------------------------------------------------------

async fn append_response_reader(
    endpoint: String,
    mut stream: tonic::Streaming<AppendEventsResponse>,
    subscribers: Arc<DashMap<i64, watch::Sender<i64>>>,
) {
    let reason = loop {
        match stream.message().await {
            Ok(Some(resp)) => {
                if resp.code == StatusCode::Ok as i32 {
                    if let Some(tx) = subscribers.get(&resp.stream_id) {
                        let _ = tx.send(resp.commit_offset);
                    }
                } else {
                    warn!(
                        endpoint = %endpoint,
                        stream_id = resp.stream_id,
                        code = resp.code,
                        "append_response_reader: non-ok response"
                    );
                }
            }
            Ok(None) => break "ss ended".to_string(),
            Err(e) => {
                warn!(endpoint = %endpoint, error = %e, "append_response_reader: error");
                break e.to_string();
            }
        }
    };
    warn!(endpoint = %endpoint, reason = %reason, "append_response_reader: ended");
}

// ---------------------------------------------------------------------------
// Read response reader — demuxes read responses by stream_id to subscribers
// ---------------------------------------------------------------------------

async fn read_response_reader(
    endpoint: String,
    mut stream: tonic::Streaming<ReadEventsResponse>,
    subscribers: Arc<DashMap<i64, mpsc::Sender<Result<ReadEventsResponse, LyraError>>>>,
) {
    let reason = loop {
        match stream.message().await {
            Ok(Some(resp)) => {
                let stream_id = resp.stream_id;
                if let Some(tx) = subscribers.get(&stream_id) {
                    match tx.try_send(Ok(resp)) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            subscribers.remove(&stream_id);
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!(
                                endpoint = %endpoint,
                                stream_id = stream_id,
                                "read subscriber full, dropping response"
                            );
                        }
                    }
                }
            }
            Ok(None) => break "ss ended".to_string(),
            Err(e) => {
                warn!(endpoint = %endpoint, error = %e, "read_response_reader: error");
                break e.to_string();
            }
        }
    };
    // Notify all subscribers with the error, then clear.
    for entry in subscribers.iter() {
        let _ = entry
            .value()
            .try_send(Err(LyraError::Transport(reason.clone())));
    }
    subscribers.clear();
    warn!(endpoint = %endpoint, reason = %reason, "read_response_reader: ended");
}
