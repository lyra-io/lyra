//! Awaitable completion for eagerly submitted operations.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::oneshot::{self, Sender};

/// The eventual result of an operation that has already been submitted.
#[must_use = "a promise must be awaited to observe the operation result"]
pub struct Promise<T, E> {
    result_rx: oneshot::Receiver<Result<T, E>>,
}

/// Completes an associated [`Promise`].
pub struct PromiseHandle<T, E> {
    result_tx: Sender<Result<T, E>>,
}

/// Indicates that a promise handle was dropped without sending a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromiseCanceled;

impl<T, E> Promise<T, E> {
    /// Creates a promise and its finishing handle.
    pub fn new() -> (PromiseHandle<T, E>, Self) {
        let (result_tx, result_rx) = oneshot::channel();
        (PromiseHandle { result_tx }, Self { result_rx })
    }
}

impl<T, E> PromiseHandle<T, E> {
    /// Completes the associated promise.
    pub fn finish(self, result: Result<T, E>) {
        let _ = self.result_tx.send(result);
    }
}

impl<T, E> Promise<T, E>
where
    E: From<PromiseCanceled>,
{
    /// Returns the completed result without waiting, or `None` while pending.
    pub fn try_result(&mut self) -> Option<Result<T, E>> {
        match self.result_rx.try_recv() {
            Ok(result) => Some(result),
            Err(TryRecvError::Closed) => Some(Err(PromiseCanceled.into())),
            Err(TryRecvError::Empty) => None,
        }
    }
}

impl<T, E> Future for Promise<T, E>
where
    E: From<PromiseCanceled>,
{
    type Output = Result<T, E>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.result_rx).poll(context) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(PromiseCanceled.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum TestError {
        Canceled,
    }

    impl From<PromiseCanceled> for TestError {
        fn from(_: PromiseCanceled) -> Self {
            Self::Canceled
        }
    }

    #[tokio::test]
    async fn resolves_the_sent_result() {
        let (handle, mut promise) = Promise::<_, TestError>::new();
        assert_eq!(promise.try_result(), None);
        handle.finish(Ok(42));

        assert_eq!(promise.await, Ok(42));
    }

    #[test]
    fn returns_a_completed_result_without_waiting() {
        let (handle, mut promise) = Promise::<_, TestError>::new();
        handle.finish(Ok(42));

        assert_eq!(promise.try_result(), Some(Ok(42)));
    }

    #[test]
    fn maps_a_dropped_handle_to_the_configured_error() {
        let (handle, mut promise) = Promise::<(), TestError>::new();
        drop(handle);

        assert_eq!(promise.try_result(), Some(Err(TestError::Canceled)));
    }
}
