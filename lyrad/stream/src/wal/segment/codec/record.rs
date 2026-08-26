//! Physical WAL record fragment encoding and decoding.

use super::super::WalError;
use super::crc::physical_checksum;
use bytes::Bytes;
use std::io::Read;
use std::path::Path;

pub(super) const HEADER_SIZE: usize = 11;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum RecordType {
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

pub(super) fn encode_fragment(
    output: &mut Vec<u8>,
    record_type: RecordType,
    segment_number: u32,
    payload: &[u8],
) {
    debug_assert!(payload.len() <= u16::MAX as usize);
    let header_start = output.len();
    output.resize(header_start + HEADER_SIZE, 0);
    output.extend_from_slice(payload);

    let checksum = physical_checksum(record_type as u8, segment_number, payload);
    output[header_start..header_start + 4].copy_from_slice(&checksum.to_le_bytes());
    output[header_start + 4..header_start + 6]
        .copy_from_slice(&(payload.len() as u16).to_le_bytes());
    output[header_start + 6] = record_type as u8;
    output[header_start + 7..header_start + 11].copy_from_slice(&segment_number.to_le_bytes());
}

pub(super) fn decode_fragment(
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
    if physical_checksum(record_type_byte, segment_number, &payload) != expected_checksum {
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

    #[test]
    fn physical_record_round_trip() {
        let mut encoded = Vec::new();
        encode_fragment(&mut encoded, RecordType::Full, 7, b"payload");

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
