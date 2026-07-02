use crate::error::ChronicleError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// StreamState — active sender + its background reader, lifecycle-tied
// ---------------------------------------------------------------------------

struct StreamState<Req: Send + 'static> {
    tx: mpsc::Sender<Req>,
    /// Wrapped in Option so graceful close can take it without triggering
    /// the abort in Drop.
    reader: Option<JoinHandle<()>>,
}

impl<Req: Send + 'static> StreamState<Req> {
    fn is_alive(&self) -> bool {
        !self.tx.is_closed() && self.reader.as_ref().is_some_and(|r| !r.is_finished())
    }
}

impl<Req: Send + 'static> Drop for StreamState<Req> {
    fn drop(&mut self) {
        if let Some(handle) = self.reader.take() {
            handle.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// Factory type — async closure that opens a new ss
// ---------------------------------------------------------------------------

/// Returns `(request_sender, response_reader_handle)`.
pub(crate) type StreamFactory<Req> = Arc<
    dyn Fn() -> Pin<
            Box<
                dyn Future<Output = Result<(mpsc::Sender<Req>, JoinHandle<()>), ChronicleError>>
                    + Send,
            >,
        > + Send
        + Sync,
>;

// ---------------------------------------------------------------------------
// RecoverableStream — lazy-init, self-recovering bidirectional gRPC ss
// ---------------------------------------------------------------------------

/// A self-recovering bidirectional gRPC ss.
///
/// Lazily opens on first [`send`]. If the ss or its response reader dies,
/// the next [`send`] transparently reopens both sides via the factory.
///
/// Clone is cheap (Arc internals) — all clones share the same underlying
/// ss and factory.
pub(crate) struct RecoverableStream<Req: Send + 'static> {
    state: Arc<Mutex<Option<StreamState<Req>>>>,
    factory: StreamFactory<Req>,
}

impl<Req: Send + 'static> Clone for RecoverableStream<Req> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            factory: self.factory.clone(),
        }
    }
}

impl<Req: Send + 'static> RecoverableStream<Req> {
    pub fn new(factory: StreamFactory<Req>) -> Self {
        Self {
            state: Arc::new(Mutex::new(None)),
            factory,
        }
    }

    /// Send a request, lazily opening or recovering the ss as needed.
    pub async fn send(&self, request: Req) -> Result<(), ChronicleError> {
        let mut guard = self.state.lock().await;
        if guard.as_ref().is_none_or(|s| !s.is_alive()) {
            // Drop the old ss — aborts the reader task.
            guard.take();
            let (tx, handle) = (self.factory)().await?;
            *guard = Some(StreamState {
                tx,
                reader: Some(handle),
            });
        }
        guard
            .as_ref()
            .unwrap()
            .tx
            .send(request)
            .await
            .map_err(|_| ChronicleError::Transport("ss closed".into()))
    }

    /// Gracefully close the ss: drop the request sender so the server
    /// sees end-of-ss, then wait for the response reader to drain
    /// remaining messages and exit.
    pub async fn close(&self) {
        let mut guard = self.state.lock().await;
        if let Some(state) = guard.as_mut() {
            // Take the reader handle out so Drop won't abort it.
            let handle = state.reader.take();
            // Drop the state (including tx) — closes the request ss.
            guard.take();
            // Wait for the reader to finish processing remaining responses.
            if let Some(h) = handle {
                let _ = h.await;
            }
        }
    }
}
