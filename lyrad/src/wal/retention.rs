use super::Sequence;
use super::error::WalError;
use crate::segment::sync_directory;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub(crate) struct SegmentMetadata {
    pub(crate) number: u64,
    pub(crate) path: PathBuf,
    pub(crate) first_sequence: Option<Sequence>,
    pub(crate) last_sequence: Option<Sequence>,
}

#[derive(Debug)]
struct SegmentState {
    metadata: SegmentMetadata,
    active: bool,
    recovery_leases: usize,
}

#[derive(Debug)]
struct RetentionState {
    segments: BTreeMap<u64, SegmentState>,
    trim_through: Option<Sequence>,
    directory_dirty: bool,
}

pub(crate) struct Retention {
    dir: PathBuf,
    state: Mutex<RetentionState>,
    has_trim: AtomicBool,
    changed: Notify,
}

#[derive(Debug)]
pub(crate) struct RecoverySegment {
    pub(crate) number: u64,
    pub(crate) path: PathBuf,
    pub(crate) tolerate_tail: bool,
}

pub(crate) struct RecoveryLease {
    retention: Arc<Retention>,
    segments: Vec<u64>,
}

impl Retention {
    pub(crate) fn new(dir: PathBuf, segments: Vec<SegmentMetadata>) -> Self {
        let segments = segments
            .into_iter()
            .map(|metadata| {
                (
                    metadata.number,
                    SegmentState {
                        metadata,
                        active: false,
                        recovery_leases: 0,
                    },
                )
            })
            .collect();
        Self {
            dir,
            state: Mutex::new(RetentionState {
                segments,
                trim_through: None,
                directory_dirty: false,
            }),
            has_trim: AtomicBool::new(false),
            changed: Notify::new(),
        }
    }

    pub(crate) fn observe_trim(&self, trim_through: Option<Sequence>) {
        let Some(trim_through) = trim_through else {
            return;
        };
        if let Ok(mut state) = self.state.lock() {
            state.trim_through = Some(
                state
                    .trim_through
                    .map_or(trim_through, |current| current.max(trim_through)),
            );
            self.has_trim.store(true, Ordering::Release);
        }
    }

    pub(crate) fn register_active(&self, number: u64, path: PathBuf) -> Result<(), WalError> {
        let mut state = self.lock()?;
        for segment in state.segments.values_mut() {
            segment.active = false;
        }
        state.segments.insert(
            number,
            SegmentState {
                metadata: SegmentMetadata {
                    number,
                    path,
                    first_sequence: None,
                    last_sequence: None,
                },
                active: true,
                recovery_leases: 0,
            },
        );
        drop(state);
        self.notify_progress();
        Ok(())
    }

    pub(crate) fn record_write(
        &self,
        number: u64,
        first_sequence: Sequence,
        last_sequence: Sequence,
    ) -> Result<(), WalError> {
        let mut state = self.lock()?;
        let segment = state.segments.get_mut(&number).ok_or_else(|| {
            WalError::Worker(format!(
                "active segment {number} is missing from retention state"
            ))
        })?;
        segment.metadata.first_sequence = segment.metadata.first_sequence.or(Some(first_sequence));
        segment.metadata.last_sequence = Some(last_sequence);
        Ok(())
    }

    pub(crate) fn lease_recovery(
        self: &Arc<Self>,
        from_sequence: Sequence,
        through_sequence: Option<Sequence>,
    ) -> Result<(Vec<RecoverySegment>, RecoveryLease), WalError> {
        let mut state = self.lock()?;
        let earliest = state
            .segments
            .values()
            .filter_map(|segment| segment.metadata.first_sequence)
            .min();
        if let Some(earliest) = earliest
            && from_sequence < earliest
        {
            return Err(WalError::SequenceExpired {
                requested: from_sequence,
                earliest,
            });
        }

        let Some(through_sequence) = through_sequence else {
            return Ok((Vec::new(), RecoveryLease::empty(Arc::clone(self))));
        };
        if from_sequence > through_sequence {
            return Ok((Vec::new(), RecoveryLease::empty(Arc::clone(self))));
        }

        let tail_number = state.segments.last_key_value().map(|(number, _)| *number);
        let selected: Vec<_> = state
            .segments
            .values()
            .filter(|segment| {
                segment
                    .metadata
                    .first_sequence
                    .zip(segment.metadata.last_sequence)
                    .is_some_and(|(first, last)| first <= through_sequence && last >= from_sequence)
            })
            .map(|segment| RecoverySegment {
                number: segment.metadata.number,
                path: segment.metadata.path.clone(),
                tolerate_tail: Some(segment.metadata.number) == tail_number,
            })
            .collect();

        for segment in &selected {
            state
                .segments
                .get_mut(&segment.number)
                .expect("selected retention segment disappeared")
                .recovery_leases += 1;
        }
        let leased = selected.iter().map(|segment| segment.number).collect();
        Ok((
            selected,
            RecoveryLease {
                retention: Arc::clone(self),
                segments: leased,
            },
        ))
    }

    pub(crate) fn trim(&self, durable_sequence: Option<Sequence>) -> Result<(), WalError> {
        let Some(durable_sequence) = durable_sequence else {
            return Ok(());
        };
        let mut state = self.lock()?;
        let Some(requested) = state.trim_through else {
            return Ok(());
        };
        let safe_sequence = requested.min(durable_sequence);
        let anchor = state
            .segments
            .values()
            .find_map(|segment| {
                segment
                    .metadata
                    .first_sequence
                    .zip(segment.metadata.last_sequence)
                    .filter(|(first, last)| *first <= durable_sequence && durable_sequence <= *last)
                    .map(|_| segment.metadata.number)
            })
            .ok_or_else(|| {
                WalError::Worker(format!(
                    "durable sequence {durable_sequence} is missing from retention state"
                ))
            })?;
        let candidates: Vec<_> = state
            .segments
            .values()
            .filter(|segment| {
                !segment.active
                    && segment.recovery_leases == 0
                    && segment.metadata.number != anchor
                    && segment
                        .metadata
                        .last_sequence
                        .is_some_and(|last| last <= safe_sequence)
            })
            .map(|segment| (segment.metadata.number, segment.metadata.path.clone()))
            .collect();

        let mut removed = Vec::new();
        let mut first_error = None;
        for (number, path) in candidates {
            match std::fs::remove_file(&path) {
                Ok(()) => removed.push(number),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    removed.push(number);
                }
                Err(error) => {
                    first_error.get_or_insert_with(|| {
                        WalError::Io(format!(
                            "failed to remove WAL segment {}: {error}",
                            path.display()
                        ))
                    });
                }
            }
        }

        for number in &removed {
            state.segments.remove(number);
        }
        state.directory_dirty |= !removed.is_empty();
        if state.directory_dirty {
            match sync_directory(&self.dir) {
                Ok(()) => state.directory_dirty = false,
                Err(error) => {
                    first_error.get_or_insert_with(|| WalError::Io(error.to_string()));
                }
            }
        }
        drop(state);

        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    pub(crate) fn notify_progress(&self) {
        if self.has_trim.load(Ordering::Acquire) {
            self.changed.notify_one();
        }
    }

    pub(crate) async fn changed(&self) {
        self.changed.notified().await;
    }

    fn release(&self, numbers: &[u64]) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let mut released = false;
        for number in numbers {
            if let Some(segment) = state.segments.get_mut(number)
                && segment.recovery_leases > 0
            {
                segment.recovery_leases -= 1;
                released |= segment.recovery_leases == 0;
            }
        }
        drop(state);
        if released {
            self.notify_progress();
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, RetentionState>, WalError> {
        self.state
            .lock()
            .map_err(|_| WalError::Worker("WAL retention state is poisoned".into()))
    }
}

impl RecoveryLease {
    fn empty(retention: Arc<Retention>) -> Self {
        Self {
            retention,
            segments: Vec::new(),
        }
    }

    pub(crate) fn release_segment(&mut self, number: u64) {
        let Some(index) = self.segments.iter().position(|segment| *segment == number) else {
            return;
        };
        self.segments.swap_remove(index);
        self.retention.release(&[number]);
    }

    pub(crate) fn release_all(&mut self) {
        let segments = std::mem::take(&mut self.segments);
        self.retention.release(&segments);
    }
}

impl Drop for RecoveryLease {
    fn drop(&mut self) {
        self.release_all();
    }
}
