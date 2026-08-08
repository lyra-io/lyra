use super::{Sequence, WalError};
use crate::segment::{self, SegmentRecord};
use bytes::Bytes;

pub(crate) use crate::segment::FILE_HEADER_SIZE;

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
