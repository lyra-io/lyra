//! Encoding and scanning for stream storage segment files.

use super::IoMode;
use super::SegmentError;
use super::io::SegmentFile;
use bytes::Bytes;
use std::path::{Path, PathBuf};

pub(crate) const ALIGNMENT: usize = 4096;
pub(crate) const BLOCK_SIZE: usize = 32 * 1024;
pub(crate) const FILE_HEADER_SIZE: usize = ALIGNMENT;
const FILE_MAGIC: &[u8; 8] = b"LYRASEG\0";
const FILE_VERSION: u16 = 2;
const FILE_HEADER_FIELDS_SIZE: usize = 32;
const PHYSICAL_HEADER_SIZE: usize = 11;
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
    pub(crate) records: Vec<Bytes>,
    pub(crate) valid_len: u64,
}

pub(crate) struct SegmentRecord<'a> {
    pub(crate) prefix: &'a [u8],
    pub(crate) payload: &'a [u8],
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
    records: &[SegmentRecord<'_>],
) -> Result<Vec<u8>, SegmentError> {
    let log_number = u32::try_from(segment_number)
        .map_err(|_| SegmentError::SegmentNumberTooLarge(segment_number))?;
    let mut output = Vec::new();

    for record in records {
        encode_logical_record(
            &mut output,
            start_offset,
            log_number,
            record.prefix,
            record.payload,
        );
    }

    let aligned_len = align_up(output.len(), ALIGNMENT);
    output.resize(aligned_len, 0);
    Ok(output)
}

fn encode_logical_record(
    output: &mut Vec<u8>,
    start_offset: u64,
    log_number: u32,
    prefix: &[u8],
    payload: &[u8],
) {
    let logical_len = prefix.len() + payload.len();
    let mut consumed = 0;
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
            prefix,
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
    prefix: &[u8],
    payload: &[u8],
    logical_offset: usize,
    fragment_len: usize,
) {
    debug_assert!(fragment_len <= u16::MAX as usize);
    let header_start = output.len();
    output.resize(header_start + PHYSICAL_HEADER_SIZE, 0);

    let mut cursor = logical_offset;
    let mut remaining = fragment_len;
    if cursor < prefix.len() {
        let prefix_len = remaining.min(prefix.len() - cursor);
        output.extend_from_slice(&prefix[cursor..cursor + prefix_len]);
        cursor += prefix_len;
        remaining -= prefix_len;
    }
    if remaining > 0 {
        let payload_offset = cursor - prefix.len();
        output.extend_from_slice(&payload[payload_offset..payload_offset + remaining]);
    }

    let fragment_start = header_start + PHYSICAL_HEADER_SIZE;
    let crc = physical_crc(
        record_type as u8,
        log_number,
        &output[fragment_start..fragment_start + fragment_len],
    );
    output[header_start..header_start + 4].copy_from_slice(&crc.to_le_bytes());
    output[header_start + 4..header_start + 6]
        .copy_from_slice(&(fragment_len as u16).to_le_bytes());
    output[header_start + 6] = record_type as u8;
    output[header_start + 7..header_start + 11].copy_from_slice(&log_number.to_le_bytes());
}

/// Streaming reader for segment files, used by the WAL to recover and read
/// back durable records.
pub(crate) struct SegmentScanner {
    path: PathBuf,
    file: SegmentFile,
    file_len: u64,
    segment_number: u64,
    expected_log_number: u32,
    tolerate_tail: bool,
    position: u64,
    last_good_end: u64,
    fragments: Vec<u8>,
    fragmented: bool,
    block_start: u64,
    block: Bytes,
    finished: bool,
}

impl SegmentScanner {
    pub(crate) fn open(
        path: &Path,
        tolerate_tail: bool,
        io_mode: IoMode,
    ) -> Result<Self, SegmentError> {
        let file = SegmentFile::open(path, io_mode)?;
        let file_len = file.len()?;
        if file_len < FILE_HEADER_SIZE as u64 {
            return corruption(path, "truncated segment file header");
        }
        let header = file.read_at(0, FILE_HEADER_SIZE)?;
        let segment_number = decode_file_header(path, &header)?;
        let expected_log_number =
            u32::try_from(segment_number).map_err(|_| SegmentError::Corruption {
                path: path.to_path_buf(),
                message: "segment number exceeds u32".into(),
            })?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
            file_len,
            segment_number,
            expected_log_number,
            tolerate_tail,
            position: FILE_HEADER_SIZE as u64,
            last_good_end: FILE_HEADER_SIZE as u64,
            fragments: Vec::new(),
            fragmented: false,
            block_start: u64::MAX,
            block: Bytes::new(),
            finished: false,
        })
    }

    pub(crate) fn segment_number(&self) -> u64 {
        self.segment_number
    }

    pub(crate) fn valid_len(&self) -> u64 {
        self.last_good_end
    }

    fn read_range(&mut self, position: u64, length: usize) -> Result<Bytes, SegmentError> {
        let data_offset = position - FILE_HEADER_SIZE as u64;
        let block_offset = data_offset % BLOCK_SIZE as u64;
        let block_start = position - block_offset;
        if self.block_start != block_start {
            let length = (self.file_len - block_start).min(BLOCK_SIZE as u64) as usize;
            self.block = self.file.read_at(block_start, length)?;
            self.block_start = block_start;
        }
        let start = (position - block_start) as usize;
        Ok(self.block.slice(start..start + length))
    }

    fn error(&self, message: impl Into<String>) -> SegmentError {
        SegmentError::Corruption {
            path: self.path.clone(),
            message: message.into(),
        }
    }

    fn tail_error(&mut self, message: impl Into<String>) -> Option<Result<Bytes, SegmentError>> {
        self.finished = true;
        if self.tolerate_tail {
            None
        } else {
            Some(Err(self.error(message)))
        }
    }

    fn hard_error(&mut self, message: impl Into<String>) -> Option<Result<Bytes, SegmentError>> {
        self.finished = true;
        Some(Err(self.error(message)))
    }
}

impl Iterator for SegmentScanner {
    type Item = Result<Bytes, SegmentError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        loop {
            if self.position >= self.file_len {
                if self.fragmented {
                    return self.tail_error("incomplete fragmented record at end of file");
                }
                self.finished = true;
                return None;
            }

            let data_offset = self.position - FILE_HEADER_SIZE as u64;
            let block_remaining = BLOCK_SIZE - (data_offset % BLOCK_SIZE as u64) as usize;
            let file_remaining = self.file_len - self.position;

            if block_remaining < PHYSICAL_HEADER_SIZE {
                let available = file_remaining.min(block_remaining as u64) as usize;
                let trailer = match self.read_range(self.position, available) {
                    Ok(trailer) => trailer,
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                };
                if !all_zero(&trailer) {
                    return self.tail_error("non-zero bytes in a block trailer");
                }
                self.position += available as u64;
                if !self.fragmented {
                    self.last_good_end = self.position;
                }
                continue;
            }

            if file_remaining < PHYSICAL_HEADER_SIZE as u64 {
                let tail = match self.read_range(self.position, file_remaining as usize) {
                    Ok(tail) => tail,
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                };
                if all_zero(&tail) && !self.fragmented {
                    self.last_good_end = self.file_len;
                    self.finished = true;
                    return None;
                }
                return self.tail_error("truncated physical record header");
            }

            let header = match self.read_range(self.position, PHYSICAL_HEADER_SIZE) {
                Ok(header) => header,
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            };
            if all_zero(&header) {
                let next_page = self
                    .position
                    .saturating_add(1)
                    .div_ceil(ALIGNMENT as u64)
                    .saturating_mul(ALIGNMENT as u64)
                    .min(self.file_len);
                let padding =
                    match self.read_range(self.position, (next_page - self.position) as usize) {
                        Ok(padding) => padding,
                        Err(error) => {
                            self.finished = true;
                            return Some(Err(error));
                        }
                    };
                if !all_zero(&padding) {
                    return self.tail_error("non-zero bytes after padding marker");
                }
                self.position = next_page;
                if !self.fragmented {
                    self.last_good_end = self.position;
                }
                continue;
            }

            let expected_crc = u32::from_le_bytes(header[..4].try_into().unwrap());
            let fragment_len = u16::from_le_bytes(header[4..6].try_into().unwrap()) as usize;
            let record_type_byte = header[6];
            let log_number = u32::from_le_bytes(header[7..11].try_into().unwrap());
            let physical_len = PHYSICAL_HEADER_SIZE + fragment_len;
            if physical_len > block_remaining || physical_len as u64 > file_remaining {
                return self.tail_error("truncated physical record body");
            }
            if log_number != self.expected_log_number {
                return self.tail_error("physical record segment number mismatch");
            }
            let record_type = match RecordType::decode(record_type_byte) {
                Ok(record_type) => record_type,
                Err(message) => return self.tail_error(message),
            };
            let fragment =
                match self.read_range(self.position + PHYSICAL_HEADER_SIZE as u64, fragment_len) {
                    Ok(fragment) => fragment,
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(error));
                    }
                };
            if physical_crc(record_type_byte, log_number, &fragment) != expected_crc {
                return self.tail_error("physical record checksum mismatch");
            }

            self.position += physical_len as u64;
            match record_type {
                RecordType::Full => {
                    if self.fragmented {
                        return self.hard_error("full record found inside fragmented record");
                    }
                    self.last_good_end = self.position;
                    return Some(Ok(fragment));
                }
                RecordType::First => {
                    if self.fragmented {
                        return self.hard_error("first fragment found inside fragmented record");
                    }
                    self.fragmented = true;
                    self.fragments.extend_from_slice(&fragment);
                }
                RecordType::Middle => {
                    if !self.fragmented {
                        return self.hard_error("middle fragment without first fragment");
                    }
                    self.fragments.extend_from_slice(&fragment);
                }
                RecordType::Last => {
                    if !self.fragmented {
                        return self.hard_error("last fragment without first fragment");
                    }
                    self.fragments.extend_from_slice(&fragment);
                    self.fragmented = false;
                    self.last_good_end = self.position;
                    return Some(Ok(Bytes::from(std::mem::take(&mut self.fragments))));
                }
            }
        }
    }
}

pub(crate) fn scan_segment(path: &Path, tolerate_tail: bool) -> Result<SegmentScan, SegmentError> {
    let mut scanner = SegmentScanner::open(path, tolerate_tail, IoMode::Standard)?;
    let segment_number = scanner.segment_number();
    let records = scanner.by_ref().collect::<Result<Vec<_>, _>>()?;
    Ok(SegmentScan {
        segment_number,
        records,
        valid_len: scanner.valid_len(),
    })
}

fn decode_file_header(path: &Path, bytes: &[u8]) -> Result<u64, SegmentError> {
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

fn physical_crc(record_type: u8, log_number: u32, payload: &[u8]) -> u32 {
    let crc = crc32c::crc32c(&[record_type]);
    let crc = crc32c::crc32c_append(crc, &log_number.to_le_bytes());
    let crc = crc32c::crc32c_append(crc, payload);
    mask_crc(crc)
}

fn mask_crc(crc: u32) -> u32 {
    crc.rotate_right(15).wrapping_add(CRC_MASK_DELTA)
}

fn corruption<T>(path: &Path, message: &str) -> Result<T, SegmentError> {
    Err(SegmentError::Corruption {
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

    fn encode_records(records: &[Bytes]) -> Vec<u8> {
        let records: Vec<_> = records
            .iter()
            .map(|payload| SegmentRecord {
                prefix: &[],
                payload,
            })
            .collect();
        encode_batch(1, FILE_HEADER_SIZE as u64, &records).unwrap()
    }

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
            Bytes::from_static(b"small"),
            Bytes::from(vec![0xAB; BLOCK_SIZE * 2 + 113]),
            Bytes::new(),
        ];
        let mut bytes = encode_file_header(1);
        bytes.extend_from_slice(&encode_records(&records));
        std::fs::write(&path, bytes).unwrap();

        let scan = scan_segment(&path, false).unwrap();
        assert_eq!(scan.records, records);
    }

    #[test]
    fn final_partial_record_can_be_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0000000001.seg");
        let records = vec![Bytes::from(vec![0xCD; BLOCK_SIZE])];
        let mut bytes = encode_file_header(1);
        bytes.extend_from_slice(&encode_records(&records));
        bytes.truncate(FILE_HEADER_SIZE + 1000);
        std::fs::write(&path, bytes).unwrap();

        let scan = scan_segment(&path, true).unwrap();
        assert!(scan.records.is_empty());
        assert_eq!(scan.valid_len, FILE_HEADER_SIZE as u64);
        assert!(scan_segment(&path, false).is_err());
    }
}
