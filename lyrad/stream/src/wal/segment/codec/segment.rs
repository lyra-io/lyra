//! WAL segment block framing and logical record assembly.

use super::super::WalError;
use super::record::{HEADER_SIZE, RecordType, decode_fragment, encode_fragment};
use bytes::Bytes;
use std::io::Read;
use std::path::Path;

const BLOCK_SIZE: usize = 32 * 1024;

pub(in crate::wal) fn encode_record(
    segment_number: u64,
    start_position: u64,
    payload: &[u8],
) -> Result<Vec<u8>, WalError> {
    let segment_number = u32::try_from(segment_number)
        .map_err(|_| WalError::SegmentNumberTooLarge(segment_number))?;
    let mut output = Vec::new();
    let mut consumed = 0;
    let mut first = true;

    loop {
        let absolute_position = start_position
            .checked_add(u64::try_from(output.len()).map_err(|_| WalError::OffsetExhausted)?)
            .ok_or(WalError::OffsetExhausted)?;
        let block_offset = usize::try_from(absolute_position % BLOCK_SIZE as u64)
            .map_err(|_| WalError::OffsetExhausted)?;
        let block_remaining = BLOCK_SIZE - block_offset;
        if block_remaining < HEADER_SIZE {
            output.resize(output.len() + block_remaining, 0);
            continue;
        }

        let available = block_remaining - HEADER_SIZE;
        let fragment_size = (payload.len() - consumed).min(available);
        let last = consumed + fragment_size == payload.len();
        let record_type = match (first, last) {
            (true, true) => RecordType::Full,
            (true, false) => RecordType::First,
            (false, true) => RecordType::Last,
            (false, false) => RecordType::Middle,
        };
        encode_fragment(
            &mut output,
            record_type,
            segment_number,
            &payload[consumed..consumed + fragment_size],
        );
        consumed += fragment_size;
        if last {
            return Ok(output);
        }
        first = false;
    }
}

pub(in crate::wal) fn decode_record(
    reader: &mut impl Read,
    path: &Path,
    segment_number: u64,
    start_position: u64,
    file_end: u64,
) -> Result<(u64, Bytes), WalError> {
    let expected_segment_number =
        u32::try_from(segment_number).map_err(|_| WalError::Corruption {
            path: path.to_path_buf(),
            message: "segment number exceeds u32".into(),
        })?;
    let mut position = start_position;
    let mut fragments = Vec::new();
    let mut fragmented = false;

    loop {
        if position >= file_end {
            return Err(WalError::truncated(
                path,
                "incomplete record at end of file",
            ));
        }

        let block_offset =
            usize::try_from(position % BLOCK_SIZE as u64).map_err(|_| WalError::OffsetExhausted)?;
        let block_remaining = BLOCK_SIZE - block_offset;
        let file_remaining = file_end - position;
        if block_remaining < HEADER_SIZE {
            let trailer_size = usize::try_from(file_remaining.min(block_remaining as u64))
                .map_err(|_| WalError::OffsetExhausted)?;
            let mut trailer = vec![0; trailer_size];
            reader.read_exact(&mut trailer)?;
            if !trailer.iter().all(|byte| *byte == 0) {
                return Err(WalError::corruption(
                    path,
                    "non-zero bytes in a block trailer",
                ));
            }
            position += trailer_size as u64;
            continue;
        }

        let (record_type, fragment, consumed) = decode_fragment(
            reader,
            path,
            expected_segment_number,
            block_remaining,
            file_remaining,
        )?;
        position += consumed;
        match record_type {
            RecordType::Full => {
                if fragmented {
                    return Err(WalError::corruption(
                        path,
                        "full record found inside fragmented record",
                    ));
                }
                return Ok((position, fragment));
            }
            RecordType::First => {
                if fragmented {
                    return Err(WalError::corruption(
                        path,
                        "first fragment found inside fragmented record",
                    ));
                }
                fragmented = true;
                fragments.extend_from_slice(&fragment);
            }
            RecordType::Middle => {
                if !fragmented {
                    return Err(WalError::corruption(
                        path,
                        "middle fragment without first fragment",
                    ));
                }
                fragments.extend_from_slice(&fragment);
            }
            RecordType::Last => {
                if !fragmented {
                    return Err(WalError::corruption(
                        path,
                        "last fragment without first fragment",
                    ));
                }
                fragments.extend_from_slice(&fragment);
                return Ok((position, Bytes::from(fragments)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn encode_records(records: &[Bytes]) -> (Vec<u8>, Vec<(u64, u64)>) {
        let mut encoded = Vec::new();
        let mut bounds = Vec::with_capacity(records.len());
        for record in records {
            let position = encoded.len() as u64;
            let encoded_record = encode_record(1, position, record).unwrap();
            let end = position + encoded_record.len() as u64;
            encoded.extend_from_slice(&encoded_record);
            bounds.push((position, end));
        }
        (encoded, bounds)
    }

    #[test]
    fn logical_records_round_trip_across_blocks() {
        let records = vec![
            Bytes::from_static(b"small"),
            Bytes::from(vec![0xAB; BLOCK_SIZE * 2 + 113]),
            Bytes::new(),
        ];
        let (encoded, bounds) = encode_records(&records);
        let file_end = encoded.len() as u64;
        let mut reader = Cursor::new(encoded);
        let decoded = bounds
            .into_iter()
            .map(|(position, end)| {
                let (next_position, payload) =
                    decode_record(&mut reader, Path::new("segment"), 1, position, file_end)
                        .unwrap();
                assert_eq!(next_position, end);
                payload
            })
            .collect::<Vec<_>>();
        assert_eq!(decoded, records);
    }

    #[test]
    fn truncated_logical_record_is_distinct_from_corruption() {
        let record = Bytes::from(vec![0xCD; BLOCK_SIZE]);
        let mut encoded = encode_record(1, 0, &record).unwrap();
        encoded.truncate(1000);
        let file_end = encoded.len() as u64;
        let error = decode_record(
            &mut Cursor::new(encoded),
            Path::new("segment"),
            1,
            0,
            file_end,
        )
        .unwrap_err();
        assert!(matches!(error, WalError::Truncated { .. }));
    }
}
