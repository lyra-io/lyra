//! WAL durability event loop.

use super::error::WalError;
use super::ops::{AdvancedSequence, AppendCompletion, DirtySegmentQueue};
use super::segment::{SegmentSyncHandle, sync_all};
use crate::vfs::VfsI;
use std::path::{Path, PathBuf};
use tokio::runtime::Handle as RuntimeHandle;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, watch};

/// Groups written records into durability boundaries and resolves promises.
pub(super) struct LogSyncer {
    // Control state
    advanced_rx: watch::Receiver<AdvancedSequence>,
    completion_rx: mpsc::Receiver<AppendCompletion>,

    // Immutable state
    dirty_segments: DirtySegmentQueue,
    wait_for_sync: bool,
    vfs: VfsI,
    dir: PathBuf,
    runtime: RuntimeHandle,

    // Mutable state
    previous_active_segment: Option<SegmentSyncHandle>,
    deferred_completion: Option<AppendCompletion>,
    segments: Vec<SegmentSyncHandle>,
    completions: Vec<AppendCompletion>,
}

impl LogSyncer {
    pub(super) fn new(
        advanced_rx: watch::Receiver<AdvancedSequence>,
        completion_rx: mpsc::Receiver<AppendCompletion>,
        dirty_segments: DirtySegmentQueue,
        wait_for_sync: bool,
        vfs: VfsI,
        dir: PathBuf,
        runtime: RuntimeHandle,
    ) -> Self {
        Self {
            // Control state
            advanced_rx,
            completion_rx,

            // Immutable state
            dirty_segments,
            wait_for_sync,
            vfs,
            dir,
            runtime,

            // Mutable state
            previous_active_segment: None,
            deferred_completion: None,
            segments: Vec::new(),
            completions: Vec::new(),
        }
    }

    pub(super) fn run(mut self) {
        while self.runtime.block_on(self.advanced_rx.changed()).is_ok() {
            let (advanced_sequence, active_segment) = self.advanced_rx.borrow_and_update().clone();
            let Some(advanced_sequence) = advanced_sequence else {
                continue;
            };
            let sync_directory = active_segment != self.previous_active_segment;
            self.previous_active_segment = active_segment.clone();

            self.segments.clear();
            self.segments
                .extend(self.dirty_segments.lock().unwrap().drain(..));
            self.segments.extend(active_segment);

            self.completions.clear();
            if let Some(completion) = self.deferred_completion.take() {
                if completion.0 <= advanced_sequence {
                    self.completions.push(completion);
                } else {
                    self.deferred_completion = Some(completion);
                }
            }
            if self.deferred_completion.is_none() {
                loop {
                    match self.completion_rx.try_recv() {
                        Ok(completion) if completion.0 <= advanced_sequence => {
                            self.completions.push(completion);
                        }
                        Ok(completion) => {
                            self.deferred_completion = Some(completion);
                            break;
                        }
                        Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                    }
                }
            }

            if !self.wait_for_sync {
                for (sequence, handle) in self.completions.drain(..) {
                    handle.finish(Ok(sequence));
                }
            }
            if let Err(error) = perform_sync(&self.segments, sync_directory, &self.vfs, &self.dir) {
                tracing::error!(error = %error, "WAL sync failed");
                for (_, handle) in self.completions.drain(..) {
                    handle.finish(Err(error.clone()));
                }
                if let Some((_, handle)) = self.deferred_completion.take() {
                    handle.finish(Err(error.clone()));
                }
                self.completion_rx.close();
                while let Some((_, handle)) = self.completion_rx.blocking_recv() {
                    handle.finish(Err(error.clone()));
                }
                break;
            }
            if self.wait_for_sync {
                for (sequence, handle) in self.completions.drain(..) {
                    handle.finish(Ok(sequence));
                }
            }
        }
    }
}

pub(super) fn perform_sync(
    segments: &[SegmentSyncHandle],
    sync_dir: bool,
    vfs: &VfsI,
    dir: &Path,
) -> Result<(), WalError> {
    let mut previous = None;
    for segment in segments.iter().rev() {
        if previous.is_some_and(|previous| previous == segment) {
            continue;
        }
        segment.sync()?;
        previous = Some(segment);
    }
    if sync_dir {
        sync_all(vfs, dir)?;
    }
    Ok(())
}
