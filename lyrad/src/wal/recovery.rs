use super::Sequence;
use super::error::WalError;
use super::format::{FILE_HEADER_SIZE, WalSegmentScanner};
use super::retention::{RecoveryLease, Retention, SegmentMetadata};
use crate::segment::{IoMode, list_segment_files, sync_directory};
use bytes::Bytes;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct RecoverySummary {
    pub(crate) last_sequence: Option<Sequence>,
    pub(crate) last_segment_number: u64,
    pub(crate) segments: Vec<SegmentMetadata>,
}

pub(crate) fn recover_directory(dir: &Path, io_mode: IoMode) -> Result<RecoverySummary, WalError> {
    let files = list_segment_files(dir)?;
    let mut last_sequence: Option<Sequence> = None;
    let mut last_segment_number = 0;
    let mut segments = Vec::with_capacity(files.len());

    for (index, (file_number, path)) in files.iter().enumerate() {
        let is_last = index + 1 == files.len();
        let mut scanner = match WalSegmentScanner::open(path, is_last, io_mode) {
            Ok(scanner) => scanner,
            Err(error) if is_last && matches!(error, WalError::Corruption { .. }) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %error,
                    "discarding torn WAL tail segment header; the segment was never durably linked"
                );
                remove_uncommitted_tail(dir, path)?;
                continue;
            }
            Err(error) => return Err(error),
        };
        if scanner.segment_number() != *file_number {
            return Err(WalError::Corruption {
                path: path.clone(),
                message: format!(
                    "segment filename number {} does not match header number {}",
                    file_number,
                    scanner.segment_number()
                ),
            });
        }

        let mut segment_first = None;
        let mut segment_last = None;
        for record in scanner.by_ref() {
            let (sequence, _) = record?;
            if let Some(previous) = last_sequence {
                let expected = previous
                    .checked_add(1)
                    .ok_or_else(|| WalError::Corruption {
                        path: path.clone(),
                        message: "WAL sequence exhausted".into(),
                    })?;
                if sequence != expected {
                    return Err(WalError::Corruption {
                        path: path.clone(),
                        message: format!(
                            "non-contiguous WAL sequence: expected {}, found {}",
                            expected, sequence
                        ),
                    });
                }
            }
            last_sequence = Some(sequence);
            segment_first = segment_first.or(Some(sequence));
            segment_last = Some(sequence);
        }

        if is_last {
            let actual_len = std::fs::metadata(path)?.len();
            let valid_len = scanner.valid_len();
            if valid_len < actual_len {
                let file = std::fs::OpenOptions::new().write(true).open(path)?;
                file.set_len(valid_len.max(FILE_HEADER_SIZE as u64))?;
                file.sync_data()?;
            }
        }
        segments.push(SegmentMetadata {
            number: *file_number,
            path: path.clone(),
            first_sequence: segment_first,
            last_sequence: segment_last,
        });
        last_segment_number = last_segment_number.max(*file_number);
    }

    Ok(RecoverySummary {
        last_sequence,
        last_segment_number,
        segments,
    })
}

fn remove_uncommitted_tail(dir: &Path, path: &Path) -> Result<(), WalError> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(WalError::Io(format!(
                "failed to remove torn WAL tail segment {}: {error}",
                path.display()
            )));
        }
    }
    sync_directory(dir)?;
    Ok(())
}

pub struct WalRecovery {
    files: Vec<super::retention::RecoverySegment>,
    file_index: usize,
    scanner: Option<(u64, WalSegmentScanner)>,
    lease: RecoveryLease,
    io_mode: IoMode,
    from_sequence: Sequence,
    through_sequence: Option<Sequence>,
    finished: bool,
}

impl WalRecovery {
    pub(crate) fn new(
        retention: &Arc<Retention>,
        from_sequence: Sequence,
        through_sequence: Option<Sequence>,
        io_mode: IoMode,
    ) -> Result<Self, WalError> {
        let (files, lease) = retention.lease_recovery(from_sequence, through_sequence)?;
        Ok(Self {
            files,
            file_index: 0,
            scanner: None,
            lease,
            io_mode,
            from_sequence,
            through_sequence,
            finished: through_sequence.is_none(),
        })
    }
}

impl Iterator for WalRecovery {
    type Item = Result<(Sequence, Bytes), WalError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let through_sequence = self.through_sequence.unwrap();

        loop {
            if let Some((segment_number, scanner)) = self.scanner.as_mut() {
                let segment_number = *segment_number;
                match scanner.next() {
                    Some(Ok((sequence, payload))) => {
                        if sequence < self.from_sequence {
                            continue;
                        }
                        if sequence > through_sequence {
                            self.finished = true;
                            self.lease.release_all();
                            return None;
                        }
                        return Some(Ok((sequence, payload)));
                    }
                    Some(Err(error)) => {
                        self.finished = true;
                        self.lease.release_all();
                        return Some(Err(error));
                    }
                    None => {
                        self.scanner = None;
                        self.lease.release_segment(segment_number);
                        continue;
                    }
                }
            }

            let Some(segment) = self.files.get(self.file_index) else {
                self.finished = true;
                self.lease.release_all();
                return None;
            };
            match WalSegmentScanner::open(&segment.path, segment.tolerate_tail, self.io_mode) {
                Ok(scanner) => {
                    self.file_index += 1;
                    self.scanner = Some((segment.number, scanner));
                }
                Err(error) => {
                    self.finished = true;
                    self.lease.release_all();
                    return Some(Err(error));
                }
            }
        }
    }
}
