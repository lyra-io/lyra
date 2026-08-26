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

/// One ordered WAL record ready for downstream publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRecord {
    sequence: Sequence,
    segment_number: u64,
    offset: u64,
    payload: Bytes,
}

impl PublishRecord {
    /// Returns the record's logical sequence.
    pub fn sequence(&self) -> Sequence {
        self.sequence
    }

    /// Returns the number of the segment containing this record.
    pub fn segment_number(&self) -> u64 {
        self.segment_number
    }

    /// Returns the record's byte position used to resume WAL recovery.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the record payload.
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

/// A contiguous, ordered batch of WAL records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishBatch {
    records: Vec<PublishRecord>,
}

impl PublishBatch {
    pub(super) fn new(records: &[(Sequence, u64, u64, Bytes)]) -> Self {
        Self {
            records: records
                .iter()
                .map(
                    |(sequence, segment_number, offset, payload)| PublishRecord {
                        sequence: *sequence,
                        segment_number: *segment_number,
                        offset: *offset,
                        payload: payload.clone(),
                    },
                )
                .collect(),
        }
    }

    /// Returns the records in sequence order.
    pub fn records(&self) -> &[PublishRecord] {
        &self.records
    }

    /// Returns the first sequence in the batch.
    pub fn first_sequence(&self) -> Sequence {
        self.records
            .first()
            .expect("publish batches are never empty")
            .sequence
    }

    /// Returns the last sequence in the batch.
    pub fn last_sequence(&self) -> Sequence {
        self.records
            .last()
            .expect("publish batches are never empty")
            .sequence
    }
}

/// Downstream state owner for durable WAL records.
#[async_trait]
pub trait PublishTarget: Send + Sync + 'static {
    /// Returns the last WAL record already reflected in the target's durable state.
    ///
    /// Implementations should persist this offset atomically with the state
    /// produced by [`apply`](Self::apply). Recovery resumes after this record.
    fn applied_offset(&self) -> Option<(u64, u64)> {
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
