use super::{Sequence, WalError};
use crate::segment::{self, IoMode, SegmentRecord, SegmentScanner};
use bytes::Bytes;
use std::path::{Path, PathBuf};

pub(crate) use crate::segment::FILE_HEADER_SIZE;

const LOGICAL_HEADER_SIZE: usize = size_of::<Sequence>();

pub(crate) struct WalSegmentScanner {
    path: PathBuf,
    inner: SegmentScanner,
}

impl WalSegmentScanner {
    pub(crate) fn open(
        path: &Path,
        tolerate_tail: bool,
        io_mode: IoMode,
    ) -> Result<Self, WalError> {
        Ok(Self {
            path: path.to_path_buf(),
            inner: SegmentScanner::open(path, tolerate_tail, io_mode)?,
        })
    }

    pub(crate) fn segment_number(&self) -> u64 {
        self.inner.segment_number()
    }

    pub(crate) fn valid_len(&self) -> u64 {
        self.inner.valid_len()
    }
}

impl Iterator for WalSegmentScanner {
    type Item = Result<(Sequence, Bytes), WalError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next()? {
            Ok(record) => Some(decode_record(&self.path, record)),
            Err(error) => Some(Err(error.into())),
        }
    }
}

pub(crate) fn encode_batch(
    segment_number: u64,
    start_offset: u64,
    records: &[(Sequence, Bytes)],
) -> Result<Vec<u8>, WalError> {
    let prefixes: Vec<_> = records
        .iter()
        .map(|(sequence, _)| sequence.to_le_bytes())
        .collect();
    let records: Vec<_> = records
        .iter()
        .zip(&prefixes)
        .map(|((_, payload), prefix)| SegmentRecord { prefix, payload })
        .collect();
    segment::encode_batch(segment_number, start_offset, &records).map_err(Into::into)
}

fn decode_record(path: &Path, mut record: Bytes) -> Result<(Sequence, Bytes), WalError> {
    if record.len() < LOGICAL_HEADER_SIZE {
        return Err(WalError::Corruption {
            path: path.to_path_buf(),
            message: "WAL sequence header is truncated".into(),
        });
    }
    let sequence = Sequence::from_le_bytes(record[..LOGICAL_HEADER_SIZE].try_into().unwrap());
    let payload = record.split_off(LOGICAL_HEADER_SIZE);
    Ok((sequence, payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_records_round_trip_through_segment_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0000000001.seg");
        let records = vec![
            (0, Bytes::from_static(b"small")),
            (1, Bytes::from(vec![0xAB; 64 * 1024 + 113])),
            (2, Bytes::new()),
        ];
        let mut bytes = segment::encode_file_header(1);
        bytes.extend_from_slice(&encode_batch(1, FILE_HEADER_SIZE as u64, &records).unwrap());
        std::fs::write(&path, bytes).unwrap();

        let scanner = WalSegmentScanner::open(&path, false, IoMode::Standard).unwrap();
        let recovered = scanner.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(recovered, records);
    }
}
