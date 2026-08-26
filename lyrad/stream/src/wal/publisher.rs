//! Application of durable WAL records to a downstream state owner.

use super::Sequence;
use super::error::WalError;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

const APPLY_RETRY_DELAY: Duration = Duration::from_millis(10);

/// A contiguous, ordered batch of WAL records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBatch {
    first_sequence: Sequence,
    payloads: Vec<Bytes>,
}

impl PublishBatch {
    pub(super) fn new(records: &[(Sequence, Bytes)]) -> Self {
        let first_sequence = records.first().expect("publish batches are never empty").0;
        debug_assert!(
            records
                .windows(2)
                .all(|records| records[0].0.checked_add(1) == Some(records[1].0))
        );
        Self {
            first_sequence,
            payloads: records.iter().map(|(_, payload)| payload.clone()).collect(),
        }
    }

    /// Returns each sequence and payload in order.
    pub fn records(
        &self,
    ) -> impl DoubleEndedIterator<Item = (Sequence, &Bytes)> + ExactSizeIterator + '_ {
        self.payloads
            .iter()
            .enumerate()
            .map(move |(index, payload)| {
                let sequence = self
                    .first_sequence
                    .checked_add(index as Sequence)
                    .expect("publish batch sequence range is valid");
                (sequence, payload)
            })
    }

    /// Returns the first sequence in the batch.
    pub fn first_sequence(&self) -> Sequence {
        self.first_sequence
    }

    /// Returns the last sequence in the batch.
    pub fn last_sequence(&self) -> Sequence {
        self.first_sequence
            .checked_add(self.payloads.len() as Sequence - 1)
            .expect("publish batch sequence range is valid")
    }
}

/// Downstream state owner for durable WAL records.
#[async_trait]
pub trait PublishTarget: Send + Sync + 'static {
    /// Returns the last WAL sequence reflected in the target's durable state.
    ///
    /// Implementations should persist this sequence atomically with the state
    /// produced by [`apply`](Self::apply). Recovery republishes later records.
    fn applied_sequence(&self) -> Option<Sequence> {
        None
    }

    async fn apply(&self, batch: PublishBatch) -> Result<(), WalError>;

    async fn close(&self) -> Result<(), WalError> {
        Ok(())
    }
}

pub(super) async fn apply_after_sync(
    context: CancellationToken,
    mut pending_rx: mpsc::Receiver<PublishBatch>,
    mut last_synced_sequence: watch::Receiver<Option<Sequence>>,
    target: Arc<dyn PublishTarget>,
) {
    while let Some(batch) = pending_rx.recv().await {
        let required_sequence = batch.last_sequence();
        if last_synced_sequence
            .wait_for(|sequence| sequence.is_some_and(|sequence| sequence >= required_sequence))
            .await
            .is_err()
        {
            tracing::error!(
                required_sequence,
                "synced-sequence watcher closed before an apply batch became durable"
            );
            break;
        }

        loop {
            match target.apply(batch.clone()).await {
                Ok(()) => {
                    break;
                }
                Err(error) if context.is_cancelled() => {
                    tracing::error!(
                        sequence = required_sequence,
                        error = %error,
                        "apply failed during close and was ignored"
                    );
                    break;
                }
                Err(error) => {
                    tracing::warn!(
                        sequence = required_sequence,
                        error = %error,
                        "apply failed; it will be retried"
                    );
                    tokio::time::sleep(APPLY_RETRY_DELAY).await;
                }
            }
        }
    }
}
