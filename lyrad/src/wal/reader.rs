use super::error::WalError;
use super::format::{FILE_HEADER_SIZE, scan_segment};
use super::io::list_segment_files;
use super::{Sequence, WalReader};
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(crate) struct RecoverySummary {
    pub(crate) earliest_sequence: Option<Sequence>,
    pub(crate) last_sequence: Option<Sequence>,
    pub(crate) last_segment_number: u64,
}

pub(crate) fn recover_directory(dir: &Path) -> Result<RecoverySummary, WalError> {
    let files = list_segment_files(dir)?;
    let mut earliest_sequence: Option<Sequence> = None;
    let mut last_sequence: Option<Sequence> = None;
    let mut last_segment_number = 0;

    for (index, (file_number, path)) in files.iter().enumerate() {
        let is_last = index + 1 == files.len();
        let scan = scan_segment(path, is_last)?;
        if scan.segment_number != *file_number {
            return Err(WalError::Corruption {
                path: path.clone(),
                message: format!(
                    "segment filename number {} does not match header number {}",
                    file_number, scan.segment_number
                ),
            });
        }

        for (sequence, _) in &scan.records {
            if let Some(previous) = last_sequence {
                let expected = previous
                    .checked_add(1)
                    .ok_or_else(|| WalError::Corruption {
                        path: path.clone(),
                        message: "WAL sequence exhausted".into(),
                    })?;
                if *sequence != expected {
                    return Err(WalError::Corruption {
                        path: path.clone(),
                        message: format!(
                            "non-contiguous WAL sequence: expected {}, found {}",
                            expected, sequence
                        ),
                    });
                }
            } else {
                earliest_sequence = Some(*sequence);
            }
            last_sequence = Some(*sequence);
        }

        if is_last {
            let actual_len = std::fs::metadata(path)?.len();
            if scan.valid_len < actual_len {
                let file = std::fs::OpenOptions::new().write(true).open(path)?;
                file.set_len(scan.valid_len.max(FILE_HEADER_SIZE as u64))?;
                file.sync_data()?;
            }
        }
        last_segment_number = last_segment_number.max(*file_number);
    }

    Ok(RecoverySummary {
        earliest_sequence,
        last_sequence,
        last_segment_number,
    })
}

pub struct SegmentWalReader {
    state: Arc<Mutex<ReaderState>>,
}

struct ReaderState {
    files: Vec<(u64, PathBuf)>,
    file_index: usize,
    buffered: VecDeque<(Sequence, Bytes)>,
    from_sequence: Sequence,
    through_sequence: Option<Sequence>,
}

impl SegmentWalReader {
    pub(crate) fn new(
        files: Vec<(u64, PathBuf)>,
        from_sequence: Sequence,
        through_sequence: Option<Sequence>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(ReaderState {
                files,
                file_index: 0,
                buffered: VecDeque::new(),
                from_sequence,
                through_sequence,
            })),
        }
    }
}

#[async_trait]
impl WalReader for SegmentWalReader {
    async fn read_next(&mut self) -> Result<Option<(Sequence, Bytes)>, WalError> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            state
                .lock()
                .map_err(|_| WalError::Worker("WAL reader lock poisoned".into()))?
                .read_next()
        })
        .await
        .map_err(|error| WalError::Worker(error.to_string()))?
    }
}

impl ReaderState {
    fn read_next(&mut self) -> Result<Option<(Sequence, Bytes)>, WalError> {
        let Some(through_sequence) = self.through_sequence else {
            return Ok(None);
        };

        loop {
            while let Some((sequence, payload)) = self.buffered.pop_front() {
                if sequence < self.from_sequence {
                    continue;
                }
                if sequence > through_sequence {
                    return Ok(None);
                }
                self.from_sequence = sequence.saturating_add(1);
                return Ok(Some((sequence, payload)));
            }

            let Some((_, path)) = self.files.get(self.file_index) else {
                return Ok(None);
            };
            let is_last = self.file_index + 1 == self.files.len();
            let scan = scan_segment(path, is_last)?;
            self.file_index += 1;
            self.buffered = scan.records.into();
        }
    }
}
