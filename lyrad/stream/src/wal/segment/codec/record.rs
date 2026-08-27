//! Logical WAL record encoding and decoding.

use super::super::WalError;
use super::crc::calculate_checksum;
use bytes::Bytes;
use std::io::Read;
use std::path::Path;

const BLOCK_SIZE: usize = 32 * 1024;
const HEADER_SIZE: usize = 11;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum RecordType {
    Full = 0,
    First = 1,
    Middle = 2,
    Last = 3,
}

impl RecordType {
    fn decode(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(Self::Full),
            1 => Ok(Self::First),
            2 => Ok(Self::Middle),
            3 => Ok(Self::Last),
            _ => Err(format!("invalid physical record type {value}")),
        }
    }
}

#[cfg(test)]
pub fn encode_record(
    segment_number: u64,
    start_position: u64,
    payload: &[u8],
) -> Result<Vec<u8>, WalError> {
    let mut output = Vec::with_capacity(payload.len().saturating_add(HEADER_SIZE));
    encode_record_parts(&mut output, segment_number, start_position, &[], payload)?;
    Ok(output)
}

pub fn encode_record_parts(
    output: &mut Vec<u8>,
    segment_number: u64,
    start_position: u64,
    prefix: &[u8],
    payload: &[u8],
) -> Result<(), WalError> {
    let segment_number = u32::try_from(segment_number)
        .map_err(|_| WalError::SegmentNumberTooLarge(segment_number))?;
    let output_start = output.len();
    let payload_size = prefix
        .len()
        .checked_add(payload.len())
        .ok_or(WalError::PositionExhausted)?;
    let mut consumed = 0;
    let mut first = true;

    loop {
        let absolute_position = start_position
            .checked_add(
                u64::try_from(output.len() - output_start)
                    .map_err(|_| WalError::PositionExhausted)?,
            )
            .ok_or(WalError::PositionExhausted)?;
        let position_in_block = usize::try_from(absolute_position % BLOCK_SIZE as u64)
            .map_err(|_| WalError::PositionExhausted)?;
        let block_remaining = BLOCK_SIZE - position_in_block;
        if block_remaining < HEADER_SIZE {
            output.resize(output.len() + block_remaining, 0);
            continue;
        }

        let available = block_remaining - HEADER_SIZE;
        let fragment_size = (payload_size - consumed).min(available);
        let last = consumed + fragment_size == payload_size;
        let record_type = match (first, last) {
            (true, true) => RecordType::Full,
            (true, false) => RecordType::First,
            (false, true) => RecordType::Last,
            (false, false) => RecordType::Middle,
        };
        encode_fragment_parts(
            output,
            record_type,
            segment_number,
            prefix,
            payload,
            consumed,
            fragment_size,
        );
        consumed += fragment_size;
        if last {
            return Ok(());
        }
        first = false;
    }
}

fn encode_fragment_parts(
    output: &mut Vec<u8>,
    record_type: RecordType,
    segment_number: u32,
    prefix: &[u8],
    payload: &[u8],
    consumed: usize,
    fragment_size: usize,
) {
    let header_start = output.len();
    output.resize(header_start + HEADER_SIZE, 0);
    let fragment_end = consumed + fragment_size;
    if consumed < prefix.len() {
        output.extend_from_slice(&prefix[consumed..fragment_end.min(prefix.len())]);
    }
    if fragment_end > prefix.len() {
        let payload_start = consumed.saturating_sub(prefix.len());
        let payload_end = fragment_end - prefix.len();
        output.extend_from_slice(&payload[payload_start..payload_end]);
    }

    let fragment_start = header_start + HEADER_SIZE;
    let checksum = calculate_checksum(record_type as u8, segment_number, &output[fragment_start..]);
    output[header_start..header_start + 4].copy_from_slice(&checksum.to_le_bytes());
    output[header_start + 4..header_start + 6]
        .copy_from_slice(&(fragment_size as u16).to_le_bytes());
    output[header_start + 6] = record_type as u8;
    output[header_start + 7..header_start + 11].copy_from_slice(&segment_number.to_le_bytes());
}

pub fn decode_record(
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

        let position_in_block = usize::try_from(position % BLOCK_SIZE as u64)
            .map_err(|_| WalError::PositionExhausted)?;
        let block_remaining = BLOCK_SIZE - position_in_block;
        let file_remaining = file_end - position;
        if block_remaining < HEADER_SIZE {
            let trailer_size = usize::try_from(file_remaining.min(block_remaining as u64))
                .map_err(|_| WalError::PositionExhausted)?;
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

fn decode_fragment(
    reader: &mut impl Read,
    path: &Path,
    expected_segment_number: u32,
    block_remaining: usize,
    file_remaining: u64,
) -> Result<(RecordType, Bytes, u64), WalError> {
    if file_remaining < HEADER_SIZE as u64 {
        return Err(WalError::truncated(
            path,
            "truncated physical record header",
        ));
    }
    let mut header = [0; HEADER_SIZE];
    reader.read_exact(&mut header)?;
    if header.iter().all(|byte| *byte == 0) {
        return Err(WalError::truncated(path, "zeroed physical record header"));
    }

    let expected_checksum = u32::from_le_bytes(header[..4].try_into().unwrap());
    let payload_size = u16::from_le_bytes(header[4..6].try_into().unwrap()) as usize;
    let record_type_byte = header[6];
    let segment_number = u32::from_le_bytes(header[7..11].try_into().unwrap());
    let record_size = HEADER_SIZE + payload_size;
    if record_size > block_remaining {
        return Err(WalError::corruption(
            path,
            "physical record crosses a block boundary",
        ));
    }
    if record_size as u64 > file_remaining {
        return Err(WalError::truncated(path, "truncated physical record body"));
    }
    if segment_number != expected_segment_number {
        return Err(WalError::corruption(
            path,
            "physical record segment number mismatch",
        ));
    }

    let record_type = RecordType::decode(record_type_byte)
        .map_err(|message| WalError::corruption(path, message))?;
    let mut payload = vec![0; payload_size];
    reader.read_exact(&mut payload)?;
    if calculate_checksum(record_type_byte, segment_number, &payload) != expected_checksum {
        return Err(WalError::corruption(
            path,
            "physical record checksum mismatch",
        ));
    }

    Ok((record_type, Bytes::from(payload), record_size as u64))
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
    fn record_parts_round_trip_across_fragments() {
        let prefix = [0xAB; 8];
        let payload = vec![0xCD; BLOCK_SIZE];
        let mut encoded = Vec::new();
        encode_record_parts(&mut encoded, 1, 0, &prefix, &payload).unwrap();

        let file_end = encoded.len() as u64;
        let (_, decoded) = decode_record(
            &mut Cursor::new(encoded),
            Path::new("segment"),
            1,
            0,
            file_end,
        )
        .unwrap();
        let mut expected = prefix.to_vec();
        expected.extend_from_slice(&payload);
        assert_eq!(decoded, expected);
    }

    #[test]
    fn record_parts_match_contiguous_encoding_at_block_boundaries() {
        let prefix = [0xAB; 8];
        for start_position in [
            0,
            BLOCK_SIZE as u64 - 5,
            BLOCK_SIZE as u64 - HEADER_SIZE as u64,
        ] {
            for payload_size in [0, 1, BLOCK_SIZE, BLOCK_SIZE * 2] {
                let payload = vec![0xCD; payload_size];
                let mut contiguous = prefix.to_vec();
                contiguous.extend_from_slice(&payload);
                let expected = encode_record(1, start_position, &contiguous).unwrap();
                let mut actual = Vec::new();
                encode_record_parts(&mut actual, 1, start_position, &prefix, &payload).unwrap();
                assert_eq!(actual, expected);
            }
        }
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

    #[test]
    fn physical_record_round_trip() {
        let encoded = encode_record(7, 0, b"payload").unwrap();

        let encoded_size = encoded.len();
        let (record_type, payload, consumed) = decode_fragment(
            &mut Cursor::new(encoded),
            Path::new("segment"),
            7,
            encoded_size,
            encoded_size as u64,
        )
        .unwrap();
        assert_eq!(record_type, RecordType::Full);
        assert_eq!(payload, Bytes::from_static(b"payload"));
        assert_eq!(consumed, encoded_size as u64);
    }
}
