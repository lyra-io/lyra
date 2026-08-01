use super::{Sequence, WalError};
use bytes::Bytes;
use std::path::Path;

pub(crate) const ALIGNMENT: usize = 4096;
pub(crate) const BLOCK_SIZE: usize = 32 * 1024;
pub(crate) const FILE_HEADER_SIZE: usize = ALIGNMENT;
const FILE_MAGIC: &[u8; 8] = b"LYRASEG\0";
const FILE_VERSION: u16 = 1;
const FILE_HEADER_FIELDS_SIZE: usize = 32;
const PHYSICAL_HEADER_SIZE: usize = 11;
const LOGICAL_HEADER_SIZE: usize = 12;
const CRC_MASK_DELTA: u32 = 0xa282_ead8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum RecordType {
    Full = 5,
    First = 6,
    Middle = 7,
    Last = 8,
}

impl RecordType {
    fn decode(value: u8) -> Result<Self, String> {
        match value {
            5 => Ok(Self::Full),
            6 => Ok(Self::First),
            7 => Ok(Self::Middle),
            8 => Ok(Self::Last),
            _ => Err(format!("invalid physical record type {value}")),
        }
    }
}

#[derive(Debug)]
pub(crate) struct SegmentScan {
    pub(crate) segment_number: u64,
    pub(crate) records: Vec<(Sequence, Bytes)>,
    pub(crate) valid_len: u64,
}

pub(crate) fn encode_file_header(segment_number: u64) -> Vec<u8> {
    let mut header = vec![0; FILE_HEADER_SIZE];
    header[0..8].copy_from_slice(FILE_MAGIC);
    header[8..10].copy_from_slice(&FILE_VERSION.to_le_bytes());
    header[10..12].copy_from_slice(&(FILE_HEADER_FIELDS_SIZE as u16).to_le_bytes());
    header[12..20].copy_from_slice(&segment_number.to_le_bytes());
    header[20..24].copy_from_slice(&(BLOCK_SIZE as u32).to_le_bytes());
    header[24..28].copy_from_slice(&(ALIGNMENT as u32).to_le_bytes());
    let crc = crc32c::crc32c(&header[..28]);
    header[28..32].copy_from_slice(&crc.to_le_bytes());
    header
}

pub(crate) fn encode_batch(
    segment_number: u64,
    start_offset: u64,
    records: &[(Sequence, Bytes)],
) -> Result<Vec<u8>, WalError> {
    let log_number = u32::try_from(segment_number)
        .map_err(|_| WalError::Worker("segment number exceeds u32".into()))?;
    let mut output = Vec::new();

    for (sequence, payload) in records {
        let payload_len = u32::try_from(payload.len()).map_err(|_| WalError::PayloadTooLarge {
            actual: payload.len(),
            maximum: u32::MAX as usize,
        })?;
        let mut logical = Vec::with_capacity(LOGICAL_HEADER_SIZE + payload.len());
        logical.extend_from_slice(&sequence.to_le_bytes());
        logical.extend_from_slice(&payload_len.to_le_bytes());
        logical.extend_from_slice(payload);
        encode_logical_record(&mut output, start_offset, log_number, logical.as_slice());
    }

    let aligned_len = align_up(output.len(), ALIGNMENT);
    output.resize(aligned_len, 0);
    Ok(output)
}

fn encode_logical_record(output: &mut Vec<u8>, start_offset: u64, log_number: u32, logical: &[u8]) {
    let mut remaining = logical;
    let mut first = true;

    loop {
        let absolute = start_offset as usize + output.len();
        let data_offset = absolute - FILE_HEADER_SIZE;
        let block_offset = data_offset % BLOCK_SIZE;
        let block_remaining = BLOCK_SIZE - block_offset;

        if block_remaining < PHYSICAL_HEADER_SIZE {
            output.resize(output.len() + block_remaining, 0);
            continue;
        }

        let available = block_remaining - PHYSICAL_HEADER_SIZE;
        let fragment_len = remaining.len().min(available);
        let last = fragment_len == remaining.len();
        let record_type = match (first, last) {
            (true, true) => RecordType::Full,
            (true, false) => RecordType::First,
            (false, true) => RecordType::Last,
            (false, false) => RecordType::Middle,
        };
        let fragment = &remaining[..fragment_len];
        encode_physical_record(output, record_type, log_number, fragment);
        remaining = &remaining[fragment_len..];

        if last {
            break;
        }
        first = false;
    }
}

fn encode_physical_record(
    output: &mut Vec<u8>,
    record_type: RecordType,
    log_number: u32,
    payload: &[u8],
) {
    debug_assert!(payload.len() <= u16::MAX as usize);
    let crc = physical_crc(record_type as u8, log_number, payload);
    output.extend_from_slice(&crc.to_le_bytes());
    output.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    output.push(record_type as u8);
    output.extend_from_slice(&log_number.to_le_bytes());
    output.extend_from_slice(payload);
}

pub(crate) fn scan_segment(path: &Path, tolerate_tail: bool) -> Result<SegmentScan, WalError> {
    let bytes = std::fs::read(path)?;
    let segment_number = decode_file_header(path, &bytes)?;
    let expected_log_number = u32::try_from(segment_number).map_err(|_| WalError::Corruption {
        path: path.to_path_buf(),
        message: "segment number exceeds u32".into(),
    })?;
    let mut records = Vec::new();
    let mut position = FILE_HEADER_SIZE;
    let mut last_good_end = FILE_HEADER_SIZE;
    let mut fragments = Vec::new();
    let mut fragmented_start = None;

    while position < bytes.len() {
        let data_offset = position - FILE_HEADER_SIZE;
        let block_remaining = BLOCK_SIZE - (data_offset % BLOCK_SIZE);
        let file_remaining = bytes.len() - position;

        if block_remaining < PHYSICAL_HEADER_SIZE {
            let available = block_remaining.min(file_remaining);
            if !all_zero(&bytes[position..position + available]) {
                return tail_or_corruption(
                    path,
                    tolerate_tail,
                    segment_number,
                    records,
                    last_good_end,
                    "non-zero bytes in a block trailer",
                );
            }
            position += available;
            if fragments.is_empty() {
                last_good_end = position;
            }
            continue;
        }

        if file_remaining < PHYSICAL_HEADER_SIZE {
            if all_zero(&bytes[position..]) && fragments.is_empty() {
                last_good_end = bytes.len();
                break;
            }
            return tail_or_corruption(
                path,
                tolerate_tail,
                segment_number,
                records,
                last_good_end,
                "truncated physical record header",
            );
        }

        if all_zero(&bytes[position..position + PHYSICAL_HEADER_SIZE]) {
            let next_page = align_up(position + 1, ALIGNMENT).min(bytes.len());
            if !all_zero(&bytes[position..next_page]) {
                return tail_or_corruption(
                    path,
                    tolerate_tail,
                    segment_number,
                    records,
                    last_good_end,
                    "non-zero bytes after padding marker",
                );
            }
            position = next_page;
            if fragments.is_empty() {
                last_good_end = position;
            }
            continue;
        }

        let expected_crc = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        let fragment_len =
            u16::from_le_bytes(bytes[position + 4..position + 6].try_into().unwrap()) as usize;
        let record_type_byte = bytes[position + 6];
        let log_number = u32::from_le_bytes(bytes[position + 7..position + 11].try_into().unwrap());
        let physical_len = PHYSICAL_HEADER_SIZE + fragment_len;

        if physical_len > block_remaining || physical_len > file_remaining {
            return tail_or_corruption(
                path,
                tolerate_tail,
                segment_number,
                records,
                last_good_end,
                "truncated physical record body",
            );
        }

        if log_number != expected_log_number {
            return tail_or_corruption(
                path,
                tolerate_tail,
                segment_number,
                records,
                last_good_end,
                "physical record segment number mismatch",
            );
        }

        let record_type = match RecordType::decode(record_type_byte) {
            Ok(record_type) => record_type,
            Err(message) => {
                return tail_or_corruption(
                    path,
                    tolerate_tail,
                    segment_number,
                    records,
                    last_good_end,
                    &message,
                );
            }
        };
        let fragment = &bytes[position + PHYSICAL_HEADER_SIZE..position + physical_len];
        let actual_crc = physical_crc(record_type_byte, log_number, fragment);
        if actual_crc != expected_crc {
            return tail_or_corruption(
                path,
                tolerate_tail,
                segment_number,
                records,
                last_good_end,
                "physical record checksum mismatch",
            );
        }

        match record_type {
            RecordType::Full => {
                if !fragments.is_empty() {
                    return corruption(path, "full record found inside fragmented record");
                }
                records.push(decode_logical_record(path, fragment)?);
                last_good_end = position + physical_len;
            }
            RecordType::First => {
                if !fragments.is_empty() || fragmented_start.is_some() {
                    return corruption(path, "first fragment found inside fragmented record");
                }
                fragmented_start = Some(position);
                fragments.extend_from_slice(fragment);
            }
            RecordType::Middle => {
                if fragments.is_empty() && fragmented_start.is_none() {
                    return corruption(path, "middle fragment without first fragment");
                }
                fragments.extend_from_slice(fragment);
            }
            RecordType::Last => {
                if fragments.is_empty() && fragmented_start.is_none() {
                    return corruption(path, "last fragment without first fragment");
                }
                fragments.extend_from_slice(fragment);
                records.push(decode_logical_record(path, &fragments)?);
                fragments.clear();
                fragmented_start = None;
                last_good_end = position + physical_len;
            }
        }
        position += physical_len;
    }

    if !fragments.is_empty() || fragmented_start.is_some() {
        return tail_or_corruption(
            path,
            tolerate_tail,
            segment_number,
            records,
            last_good_end,
            "incomplete fragmented record at end of file",
        );
    }

    Ok(SegmentScan {
        segment_number,
        records,
        valid_len: last_good_end as u64,
    })
}

fn decode_file_header(path: &Path, bytes: &[u8]) -> Result<u64, WalError> {
    if bytes.len() < FILE_HEADER_SIZE {
        return corruption(path, "truncated segment file header");
    }
    if &bytes[..8] != FILE_MAGIC {
        return corruption(path, "invalid segment file magic");
    }
    let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    if version != FILE_VERSION {
        return corruption(path, &format!("unsupported segment version {version}"));
    }
    let header_size = u16::from_le_bytes(bytes[10..12].try_into().unwrap()) as usize;
    if header_size != FILE_HEADER_FIELDS_SIZE {
        return corruption(path, "invalid segment header size");
    }
    let block_size = u32::from_le_bytes(bytes[20..24].try_into().unwrap()) as usize;
    let alignment = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
    if block_size != BLOCK_SIZE || alignment != ALIGNMENT {
        return corruption(path, "unsupported segment block size or alignment");
    }
    let expected_crc = u32::from_le_bytes(bytes[28..32].try_into().unwrap());
    let actual_crc = crc32c::crc32c(&bytes[..28]);
    if actual_crc != expected_crc {
        return corruption(path, "segment header checksum mismatch");
    }
    Ok(u64::from_le_bytes(bytes[12..20].try_into().unwrap()))
}

fn decode_logical_record(path: &Path, bytes: &[u8]) -> Result<(Sequence, Bytes), WalError> {
    if bytes.len() < LOGICAL_HEADER_SIZE {
        return corruption(path, "logical record header is truncated");
    }
    let sequence = u64::from_le_bytes(bytes[..8].try_into().unwrap());
    let payload_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    if bytes.len() != LOGICAL_HEADER_SIZE + payload_len {
        return corruption(path, "logical record payload length mismatch");
    }
    Ok((
        sequence,
        Bytes::copy_from_slice(&bytes[LOGICAL_HEADER_SIZE..]),
    ))
}

fn physical_crc(record_type: u8, log_number: u32, payload: &[u8]) -> u32 {
    let crc = crc32c::crc32c(&[record_type]);
    let crc = crc32c::crc32c_append(crc, &log_number.to_le_bytes());
    let crc = crc32c::crc32c_append(crc, payload);
    mask_crc(crc)
}

fn mask_crc(crc: u32) -> u32 {
    crc.rotate_right(15).wrapping_add(CRC_MASK_DELTA)
}

fn tail_or_corruption(
    path: &Path,
    tolerate_tail: bool,
    segment_number: u64,
    records: Vec<(Sequence, Bytes)>,
    valid_len: usize,
    message: &str,
) -> Result<SegmentScan, WalError> {
    if tolerate_tail {
        Ok(SegmentScan {
            segment_number,
            records,
            valid_len: valid_len as u64,
        })
    } else {
        corruption(path, message)
    }
}

fn corruption<T>(path: &Path, message: &str) -> Result<T, WalError> {
    Err(WalError::Corruption {
        path: path.to_path_buf(),
        message: message.to_owned(),
    })
}

fn all_zero(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| *byte == 0)
}

fn align_up(value: usize, alignment: usize) -> usize {
    value.div_ceil(alignment) * alignment
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0000000007.seg");
        std::fs::write(&path, encode_file_header(7)).unwrap();
        let scan = scan_segment(&path, false).unwrap();
        assert_eq!(scan.segment_number, 7);
        assert!(scan.records.is_empty());
    }

    #[test]
    fn records_round_trip_across_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0000000001.seg");
        let records = vec![
            (0, Bytes::from_static(b"small")),
            (1, Bytes::from(vec![0xAB; BLOCK_SIZE * 2 + 113])),
            (2, Bytes::new()),
        ];
        let mut bytes = encode_file_header(1);
        bytes.extend_from_slice(&encode_batch(1, FILE_HEADER_SIZE as u64, &records).unwrap());
        std::fs::write(&path, bytes).unwrap();

        let scan = scan_segment(&path, false).unwrap();
        assert_eq!(scan.records, records);
    }

    #[test]
    fn final_partial_record_can_be_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0000000001.seg");
        let records = vec![(0, Bytes::from(vec![0xCD; BLOCK_SIZE]))];
        let mut bytes = encode_file_header(1);
        bytes.extend_from_slice(&encode_batch(1, FILE_HEADER_SIZE as u64, &records).unwrap());
        bytes.truncate(FILE_HEADER_SIZE + 1000);
        std::fs::write(&path, bytes).unwrap();

        let scan = scan_segment(&path, true).unwrap();
        assert!(scan.records.is_empty());
        assert_eq!(scan.valid_len, FILE_HEADER_SIZE as u64);
        assert!(scan_segment(&path, false).is_err());
    }
}
