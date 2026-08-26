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

/// Indicates that a promise producer disappeared without sending a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PromiseDisconnected;

impl<T, E> Promise<T, E> {
    /// Creates a promise and the sender used to complete it.
    pub fn new() -> (Sender<Result<T, E>>, Self) {
        let (result_tx, result_rx) = oneshot::channel();
        (result_tx, Self { result_rx })
    }
}

impl<T, E> Promise<T, E>
where
    E: From<PromiseDisconnected>,
{
    /// Returns the completed result without waiting, or `None` while pending.
    pub fn try_result(&mut self) -> Option<Result<T, E>> {
        match self.result_rx.try_recv() {
            Ok(result) => Some(result),
            Err(TryRecvError::Closed) => Some(Err(PromiseDisconnected.into())),
            Err(TryRecvError::Empty) => None,
        }
    }
}

impl<T, E> Future for Promise<T, E>
where
    E: From<PromiseDisconnected>,
{
    type Output = Result<T, E>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.result_rx).poll(context) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(PromiseDisconnected.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    enum TestError {
        Disconnected,
    }

    impl From<PromiseDisconnected> for TestError {
        fn from(_: PromiseDisconnected) -> Self {
            Self::Disconnected
        }
    }

    #[tokio::test]
    async fn resolves_the_sent_result() {
        let (result_tx, mut promise) = Promise::<_, TestError>::new();
        assert_eq!(promise.try_result(), None);
        result_tx.send(Ok(42)).unwrap();

        assert_eq!(promise.await, Ok(42));
    }

    #[test]
    fn returns_a_completed_result_without_waiting() {
        let (result_tx, mut promise) = Promise::<_, TestError>::new();
        result_tx.send(Ok(42)).unwrap();

        assert_eq!(promise.try_result(), Some(Ok(42)));
    }

    #[test]
    fn maps_a_dropped_sender_to_the_configured_error() {
        let (result_tx, mut promise) = Promise::<(), TestError>::new();
        drop(result_tx);

        assert_eq!(promise.try_result(), Some(Err(TestError::Disconnected)));
    }
}
