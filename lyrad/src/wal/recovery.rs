use super::Sequence;
use super::error::WalError;
use super::format::{FILE_HEADER_SIZE, WalSegmentScanner};
use crate::segment::{IoMode, list_segment_files};
use bytes::Bytes;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(crate) struct RecoverySummary {
    pub(crate) earliest_sequence: Option<Sequence>,
    pub(crate) last_sequence: Option<Sequence>,
    pub(crate) last_segment_number: u64,
}

pub(crate) fn recover_directory(dir: &Path, io_mode: IoMode) -> Result<RecoverySummary, WalError> {
    let files = list_segment_files(dir)?;
    let mut earliest_sequence: Option<Sequence> = None;
    let mut last_sequence: Option<Sequence> = None;
    let mut last_segment_number = 0;

    for (index, (file_number, path)) in files.iter().enumerate() {
        let is_last = index + 1 == files.len();
        let mut scanner = WalSegmentScanner::open(path, is_last, io_mode)?;
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
            } else {
                earliest_sequence = Some(sequence);
            }
            last_sequence = Some(sequence);
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
        last_segment_number = last_segment_number.max(*file_number);
    }

    Ok(RecoverySummary {
        earliest_sequence,
        last_sequence,
        last_segment_number,
    })
}

pub struct WalRecovery {
    files: Vec<(u64, PathBuf)>,
    file_index: usize,
    scanner: Option<WalSegmentScanner>,
    io_mode: IoMode,
    from_sequence: Sequence,
    through_sequence: Option<Sequence>,
    finished: bool,
}

impl WalRecovery {
    pub(crate) fn new(
        dir: &Path,
        from_sequence: Sequence,
        through_sequence: Option<Sequence>,
        io_mode: IoMode,
    ) -> Result<Self, WalError> {
        Ok(Self {
            files: list_segment_files(dir)?,
            file_index: 0,
            scanner: None,
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
            if let Some(scanner) = self.scanner.as_mut() {
                match scanner.next() {
                    Some(Ok((sequence, payload))) => {
                        if sequence < self.from_sequence {
                            continue;
                        }
                        if sequence > through_sequence {
                            self.finished = true;
                            return None;
                        }
                        return Some(Ok((sequence, payload)));
                    }
                    Some(Err(error)) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    None => {
                        self.scanner = None;
                        continue;
                    }
                }
            }

            let Some((_, path)) = self.files.get(self.file_index) else {
                self.finished = true;
                return None;
            };
            let is_last = self.file_index + 1 == self.files.len();
            match WalSegmentScanner::open(path, is_last, self.io_mode) {
                Ok(scanner) => {
                    self.file_index += 1;
                    self.scanner = Some(scanner);
                }
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            }
        }
    }
}
