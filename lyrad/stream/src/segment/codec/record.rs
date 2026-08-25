//! Logical and physical record encoding, decoding, and recovery scanning.

#[cfg(test)]
use super::super::IoMode;
use super::super::SegmentError;
#[cfg(test)]
use super::super::vfs::open_local_file;
use super::super::vfs::{IoFile, VfsFile};
use super::crc::physical_checksum;
use super::segment::{FILE_HEADER_SIZE, align_up, read_file_header, validate_alignment};
use bytes::Bytes;
use std::path::Path;
use std::sync::Arc;

pub(super) const BLOCK_SIZE: usize = 32 * 1024;
pub(crate) const PHYSICAL_HEADER_SIZE: usize = 11;

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
    pub(crate) index: Vec<u64>,
    pub(crate) valid_len: u64,
}

struct DecodedRecord {
    payload: Bytes,
    next_position: u64,
}

pub(crate) fn encode_record(
    segment_number: u64,
    start_position: u64,
    payload: &[u8],
    alignment: usize,
) -> Result<Vec<u8>, SegmentError> {
    validate_alignment(alignment)?;
    let log_number = u32::try_from(segment_number)
        .map_err(|_| SegmentError::SegmentNumberTooLarge(segment_number))?;
    let mut output = Vec::new();
    encode_logical_record(&mut output, start_position, log_number, payload);

    let aligned_len = align_up(output.len(), alignment);
    output.resize(aligned_len, 0);
    Ok(output)
}

pub(crate) fn scan_file(
    file: Arc<VfsFile>,
    tolerate_tail: bool,
) -> Result<SegmentScan, SegmentError> {
    let header = read_file_header(&file)?;
    let file_len = file.size()?;
    let mut position = FILE_HEADER_SIZE as u64;
    let mut index = Vec::new();

    while position < file_len {
        match decode_record0(
            &file,
            header.segment_number,
            position,
            file_len,
            header.alignment,
        ) {
            Ok(Some(record)) => {
                index.push(position);
                position = record.next_position;
            }
            Ok(None) => break,
            Err(_) if tolerate_tail => break,
            Err(error) => return Err(error),
        }
    }

    Ok(SegmentScan {
        segment_number: header.segment_number,
        index,
        valid_len: position,
    })
}

pub(crate) fn read_record(
    file: &Arc<VfsFile>,
    segment_number: u64,
    position: u64,
    end: u64,
    alignment: usize,
) -> Result<Bytes, SegmentError> {
    let Some(record) = decode_record0(file, segment_number, position, end, alignment)? else {
        return corruption(file.path(), "segment index points to no record");
    };
    if record.next_position != end {
        return corruption(file.path(), "segment index does not bound one record");
    }
    Ok(record.payload)
}

fn encode_logical_record(
    output: &mut Vec<u8>,
    start_position: u64,
    log_number: u32,
    payload: &[u8],
) {
    let logical_len = payload.len();
    let mut consumed = 0;
    let mut first = true;

    loop {
        let absolute = start_position as usize + output.len();
        let data_offset = absolute - FILE_HEADER_SIZE;
        let block_offset = data_offset % BLOCK_SIZE;
        let block_remaining = BLOCK_SIZE - block_offset;

        if block_remaining < PHYSICAL_HEADER_SIZE {
            output.resize(output.len() + block_remaining, 0);
            continue;
        }

        let available = block_remaining - PHYSICAL_HEADER_SIZE;
        let fragment_len = (logical_len - consumed).min(available);
        let last = consumed + fragment_len == logical_len;
        let record_type = match (first, last) {
            (true, true) => RecordType::Full,
            (true, false) => RecordType::First,
            (false, true) => RecordType::Last,
            (false, false) => RecordType::Middle,
        };
        encode_physical_record(
            output,
            record_type,
            log_number,
            payload,
            consumed,
            fragment_len,
        );
        consumed += fragment_len;

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
    logical_offset: usize,
    fragment_len: usize,
) {
    debug_assert!(fragment_len <= u16::MAX as usize);
    let header_start = output.len();
    output.resize(header_start + PHYSICAL_HEADER_SIZE, 0);
    output.extend_from_slice(&payload[logical_offset..logical_offset + fragment_len]);

    let fragment_start = header_start + PHYSICAL_HEADER_SIZE;
    let checksum = physical_checksum(
        record_type as u8,
        log_number,
        &output[fragment_start..fragment_start + fragment_len],
    );
    output[header_start..header_start + 4].copy_from_slice(&checksum.to_le_bytes());
    output[header_start + 4..header_start + 6]
        .copy_from_slice(&(fragment_len as u16).to_le_bytes());
    output[header_start + 6] = record_type as u8;
    output[header_start + 7..header_start + 11].copy_from_slice(&log_number.to_le_bytes());
}

fn decode_record0(
    file: &VfsFile,
    segment_number: u64,
    start_position: u64,
    file_end: u64,
    alignment: usize,
) -> Result<Option<DecodedRecord>, SegmentError> {
    validate_alignment(alignment)?;
    let expected_log_number =
        u32::try_from(segment_number).map_err(|_| SegmentError::Corruption {
            path: file.path().to_path_buf(),
            message: "segment number exceeds u32".into(),
        })?;
    let mut position = start_position;
    let mut fragments = Vec::new();
    let mut fragmented = false;

    loop {
        if position >= file_end {
            return corruption(file.path(), "incomplete record at end of file");
        }

        let data_offset = position
            .checked_sub(FILE_HEADER_SIZE as u64)
            .ok_or_else(|| corruption_error(file.path(), "record starts before file header"))?;
        let block_remaining = BLOCK_SIZE - (data_offset % BLOCK_SIZE as u64) as usize;
        let file_remaining = file_end - position;

        if block_remaining < PHYSICAL_HEADER_SIZE {
            let available = file_remaining.min(block_remaining as u64) as usize;
            let trailer = file.read_at(position, available)?;
            if !all_zero(&trailer) {
                return corruption(file.path(), "non-zero bytes in a block trailer");
            }
            position += available as u64;
            continue;
        }

        if file_remaining < PHYSICAL_HEADER_SIZE as u64 {
            return corruption(file.path(), "truncated physical record header");
        }

        let header = file.read_at(position, PHYSICAL_HEADER_SIZE)?;
        if all_zero(&header) {
            if fragmented {
                return corruption(file.path(), "incomplete fragmented record");
            }
            return Ok(None);
        }

        let expected_checksum = u32::from_le_bytes(header[..4].try_into().unwrap());
        let fragment_len = u16::from_le_bytes(header[4..6].try_into().unwrap()) as usize;
        let record_type_byte = header[6];
        let log_number = u32::from_le_bytes(header[7..11].try_into().unwrap());
        let physical_len = PHYSICAL_HEADER_SIZE + fragment_len;
        if physical_len > block_remaining || physical_len as u64 > file_remaining {
            return corruption(file.path(), "truncated physical record body");
        }
        if log_number != expected_log_number {
            return corruption(file.path(), "physical record segment number mismatch");
        }
        let record_type = RecordType::decode(record_type_byte)
            .map_err(|message| corruption_error(file.path(), &message))?;
        let fragment = file.read_at(position + PHYSICAL_HEADER_SIZE as u64, fragment_len)?;
        if physical_checksum(record_type_byte, log_number, &fragment) != expected_checksum {
            return corruption(file.path(), "physical record checksum mismatch");
        }

        position += physical_len as u64;
        match record_type {
            RecordType::Full => {
                if fragmented {
                    return corruption(file.path(), "full record found inside fragmented record");
                }
                return finish_record(file, fragment, position, file_end, alignment).map(Some);
            }
            RecordType::First => {
                if fragmented {
                    return corruption(
                        file.path(),
                        "first fragment found inside fragmented record",
                    );
                }
                fragmented = true;
                fragments.extend_from_slice(&fragment);
            }
            RecordType::Middle => {
                if !fragmented {
                    return corruption(file.path(), "middle fragment without first fragment");
                }
                fragments.extend_from_slice(&fragment);
            }
            RecordType::Last => {
                if !fragmented {
                    return corruption(file.path(), "last fragment without first fragment");
                }
                fragments.extend_from_slice(&fragment);
                return finish_record(file, Bytes::from(fragments), position, file_end, alignment)
                    .map(Some);
            }
        }
    }
}

fn finish_record(
    file: &VfsFile,
    payload: Bytes,
    position: u64,
    file_end: u64,
    alignment: usize,
) -> Result<DecodedRecord, SegmentError> {
    let next_position = position
        .checked_next_multiple_of(alignment as u64)
        .ok_or(SegmentError::OffsetExhausted)?;
    if next_position > file_end {
        return corruption(file.path(), "truncated record alignment padding");
    }
    let padding_len =
        usize::try_from(next_position - position).map_err(|_| SegmentError::OffsetExhausted)?;
    if padding_len != 0 && !all_zero(&file.read_at(position, padding_len)?) {
        return corruption(file.path(), "non-zero bytes in record alignment padding");
    }
    Ok(DecodedRecord {
        payload,
        next_position,
    })
}

fn corruption<T>(path: &Path, message: &str) -> Result<T, SegmentError> {
    Err(corruption_error(path, message))
}

fn corruption_error(path: &Path, message: &str) -> SegmentError {
    SegmentError::Corruption {
        path: path.to_path_buf(),
        message: message.to_owned(),
    }
}

fn all_zero(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| *byte == 0)
}

#[cfg(test)]
mod tests {
    use super::super::segment::{ALIGNMENT, encode_file_header};
    use super::*;
    use bytes::Bytes;

    fn encode_records(records: &[Bytes]) -> Vec<u8> {
        let mut encoded = Vec::new();
        for record in records {
            let position = FILE_HEADER_SIZE as u64 + encoded.len() as u64;
            encoded.extend_from_slice(&encode_record(1, position, record, ALIGNMENT).unwrap());
        }
        encoded
    }

    fn scan_records(path: &Path, tolerate_tail: bool) -> Result<Vec<Bytes>, SegmentError> {
        let (_, file) = open_local_file(path, IoMode::Standard)?;
        let file = Arc::new(file);
        let scan = scan_file(Arc::clone(&file), tolerate_tail)?;
        scan.index
            .iter()
            .enumerate()
            .map(|(index, position)| {
                let end = scan.index.get(index + 1).copied().unwrap_or(scan.valid_len);
                read_record(&file, scan.segment_number, *position, end, ALIGNMENT)
            })
            .collect()
    }

    #[test]
    fn records_round_trip_across_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0000000001.seg");
        let records = vec![
            Bytes::from_static(b"small"),
            Bytes::from(vec![0xAB; BLOCK_SIZE * 2 + 113]),
            Bytes::new(),
        ];
        let mut bytes = encode_file_header(1, ALIGNMENT).unwrap();
        bytes.extend_from_slice(&encode_records(&records));
        std::fs::write(&path, bytes).unwrap();

        assert_eq!(scan_records(&path, false).unwrap(), records);
    }

    #[test]
    fn final_partial_record_can_be_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0000000001.seg");
        let records = vec![Bytes::from(vec![0xCD; BLOCK_SIZE])];
        let mut bytes = encode_file_header(1, ALIGNMENT).unwrap();
        bytes.extend_from_slice(&encode_records(&records));
        bytes.truncate(FILE_HEADER_SIZE + 1000);
        std::fs::write(&path, bytes).unwrap();

        let scan = {
            let (_, file) = open_local_file(&path, IoMode::Standard).unwrap();
            scan_file(Arc::new(file), true).unwrap()
        };
        assert!(scan.index.is_empty());
        assert_eq!(scan.valid_len, FILE_HEADER_SIZE as u64);

        let (_, file) = open_local_file(&path, IoMode::Standard).unwrap();
        assert!(scan_file(Arc::new(file), false).is_err());
    }
}
