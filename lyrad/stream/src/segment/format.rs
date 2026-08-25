//! Encoding and scanning for stream storage segment files.

#[cfg(test)]
use super::IoMode;
use super::SegmentError;
#[cfg(test)]
use super::vfs::open_local_file;
use super::vfs::{IoFile, VfsFile};
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) const ALIGNMENT: usize = 4096;
pub(crate) const BLOCK_SIZE: usize = 32 * 1024;
pub(crate) const FILE_HEADER_SIZE: usize = ALIGNMENT;
const FILE_MAGIC: &[u8; 8] = b"LYRASEG\0";
const FILE_VERSION: u16 = 3;
const FILE_HEADER_FIELDS_SIZE: usize = 32;
const FOOTER_MAGIC: &[u8; 8] = b"LYRAIDX\0";
const FOOTER_VERSION: u16 = 1;
const FOOTER_FIELDS_SIZE: usize = 52;
const FOOTER_SIZE: usize = ALIGNMENT;
const INDEX_ENTRY_SIZE: usize = size_of::<u64>();
pub(crate) const PHYSICAL_HEADER_SIZE: usize = 11;
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
    pub(crate) index: Vec<u64>,
    pub(crate) valid_len: u64,
}

pub(crate) struct LoadedIndex {
    pub(crate) segment_number: u64,
    pub(crate) records_size: u64,
    pub(crate) index: Vec<u64>,
}

struct SegmentFooter {
    segment_number: u64,
    records_size: u64,
    record_count: u64,
    index_size: u64,
    index_checksum: u32,
}

struct ScannedRecord {
    position: u64,
    payload: Bytes,
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

pub(crate) fn encode_index_footer(
    segment_number: u64,
    records_size: u64,
    index: &[u64],
) -> Result<Vec<u8>, SegmentError> {
    let entries_size = index
        .len()
        .checked_mul(INDEX_ENTRY_SIZE)
        .ok_or(SegmentError::OffsetExhausted)?;
    let index_size = if entries_size == 0 {
        0
    } else {
        align_up(entries_size, ALIGNMENT)
    };
    let tail_size = index_size
        .checked_add(FOOTER_SIZE)
        .ok_or(SegmentError::OffsetExhausted)?;
    let mut output = vec![0; tail_size];
    for (entry, bytes) in index.iter().zip(output[..entries_size].chunks_exact_mut(8)) {
        bytes.copy_from_slice(&entry.to_le_bytes());
    }

    let (index_bytes, footer) = output.split_at_mut(index_size);
    footer[..8].copy_from_slice(FOOTER_MAGIC);
    footer[8..10].copy_from_slice(&FOOTER_VERSION.to_le_bytes());
    footer[10..12].copy_from_slice(&(FOOTER_FIELDS_SIZE as u16).to_le_bytes());
    footer[12..20].copy_from_slice(&segment_number.to_le_bytes());
    footer[20..28].copy_from_slice(&records_size.to_le_bytes());
    footer[28..36].copy_from_slice(
        &u64::try_from(index.len())
            .map_err(|_| SegmentError::OffsetExhausted)?
            .to_le_bytes(),
    );
    footer[36..44].copy_from_slice(
        &u64::try_from(index_size)
            .map_err(|_| SegmentError::OffsetExhausted)?
            .to_le_bytes(),
    );
    footer[44..48].copy_from_slice(&crc32c::crc32c(index_bytes).to_le_bytes());
    let footer_checksum = crc32c::crc32c(&footer[..48]);
    footer[48..52].copy_from_slice(&footer_checksum.to_le_bytes());
    Ok(output)
}

pub(crate) fn load_index(file: &VfsFile) -> Result<Option<LoadedIndex>, SegmentError> {
    let file_len = file.size()?;
    let Some(footer_start) = file_len.checked_sub(FOOTER_SIZE as u64) else {
        return Ok(None);
    };
    let footer_bytes = file.read_at(footer_start, FOOTER_SIZE)?;
    let Some(footer) = decode_footer(&footer_bytes) else {
        return Ok(None);
    };

    let entries_size = footer
        .record_count
        .checked_mul(INDEX_ENTRY_SIZE as u64)
        .ok_or_else(|| corruption_error(file.path(), "segment index size overflows u64"))?;
    let expected_index_size = if entries_size == 0 {
        0
    } else {
        entries_size
            .checked_next_multiple_of(ALIGNMENT as u64)
            .ok_or_else(|| corruption_error(file.path(), "aligned segment index size overflows"))?
    };
    if footer.index_size != expected_index_size {
        return Ok(None);
    }

    let Some(index_start) = footer_start.checked_sub(footer.index_size) else {
        return Ok(None);
    };
    if !footer.records_size.is_multiple_of(ALIGNMENT as u64) {
        return Ok(None);
    }
    let expected_index_start = (FILE_HEADER_SIZE as u64)
        .checked_add(footer.records_size)
        .ok_or_else(|| corruption_error(file.path(), "segment records size overflows u64"))?;
    if index_start != expected_index_start {
        return Ok(None);
    }

    let index_size = usize::try_from(footer.index_size)
        .map_err(|_| corruption_error(file.path(), "segment index does not fit memory"))?;
    let index_bytes = file.read_at(index_start, index_size)?;
    if crc32c::crc32c(&index_bytes) != footer.index_checksum {
        return Ok(None);
    }
    let entries_size = usize::try_from(entries_size)
        .map_err(|_| corruption_error(file.path(), "segment index entries do not fit memory"))?;
    if !all_zero(&index_bytes[entries_size..]) {
        return Ok(None);
    }

    let mut index =
        Vec::with_capacity(usize::try_from(footer.record_count).map_err(|_| {
            corruption_error(file.path(), "segment record count does not fit memory")
        })?);
    for bytes in index_bytes[..entries_size].chunks_exact(INDEX_ENTRY_SIZE) {
        index.push(u64::from_le_bytes(bytes.try_into().unwrap()));
    }
    if !valid_index(&index, index_start) {
        return Ok(None);
    }

    Ok(Some(LoadedIndex {
        segment_number: footer.segment_number,
        records_size: footer.records_size,
        index,
    }))
}

pub(crate) fn read_file_header(file: &VfsFile) -> Result<u64, SegmentError> {
    if file.size()? < FILE_HEADER_SIZE as u64 {
        return corruption(file.path(), "truncated segment file header");
    }
    let header = file.read_at(0, FILE_HEADER_SIZE)?;
    decode_file_header(file.path(), &header)
}

pub(crate) fn encode_record(
    segment_number: u64,
    start_offset: u64,
    payload: &[u8],
) -> Result<Vec<u8>, SegmentError> {
    let log_number = u32::try_from(segment_number)
        .map_err(|_| SegmentError::SegmentNumberTooLarge(segment_number))?;
    let mut output = Vec::new();
    encode_logical_record(&mut output, start_offset, log_number, payload);

    let aligned_len = align_up(output.len(), ALIGNMENT);
    output.resize(aligned_len, 0);
    Ok(output)
}

fn encode_logical_record(output: &mut Vec<u8>, start_offset: u64, log_number: u32, payload: &[u8]) {
    let logical_len = payload.len();
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
struct SegmentScanner {
    // Immutable state
    path: PathBuf,
    file: Arc<VfsFile>,
    file_len: u64,
    segment_number: u64,
    expected_log_number: u32,
    tolerate_tail: bool,

    // Mutable state
    position: u64,
    last_good_end: u64,
    fragments: Vec<u8>,
    fragmented: bool,
    record_position: u64,
    block_start: u64,
    block: Bytes,
    finished: bool,
}

impl SegmentScanner {
    #[cfg(test)]
    fn open(path: &Path, tolerate_tail: bool, io_mode: IoMode) -> Result<Self, SegmentError> {
        let file = Arc::new(open_local_file(path, io_mode)?);
        Self::open0(file, tolerate_tail)
    }

    pub(crate) fn open0(file: Arc<VfsFile>, tolerate_tail: bool) -> Result<Self, SegmentError> {
        let path = file.path();
        let file_len = file.size()?;
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
            record_position: FILE_HEADER_SIZE as u64,
            block_start: u64::MAX,
            block: Bytes::new(),
            finished: false,
        })
    }

    fn range(
        file: Arc<VfsFile>,
        segment_number: u64,
        position: u64,
        end: u64,
    ) -> Result<Self, SegmentError> {
        let expected_log_number =
            u32::try_from(segment_number).map_err(|_| SegmentError::Corruption {
                path: file.path().to_path_buf(),
                message: "segment number exceeds u32".into(),
            })?;
        Ok(Self {
            path: file.path().to_path_buf(),
            file,
            file_len: end,
            segment_number,
            expected_log_number,
            tolerate_tail: false,
            position,
            last_good_end: position,
            fragments: Vec::new(),
            fragmented: false,
            record_position: position,
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

    fn tail_error(
        &mut self,
        message: impl Into<String>,
    ) -> Option<Result<ScannedRecord, SegmentError>> {
        self.finished = true;
        if self.tolerate_tail {
            None
        } else {
            Some(Err(self.error(message)))
        }
    }

    fn hard_error(
        &mut self,
        message: impl Into<String>,
    ) -> Option<Result<ScannedRecord, SegmentError>> {
        self.finished = true;
        Some(Err(self.error(message)))
    }
}

impl Iterator for SegmentScanner {
    type Item = Result<ScannedRecord, SegmentError>;

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
                    return Some(Ok(ScannedRecord {
                        position: self.position - physical_len as u64,
                        payload: fragment,
                    }));
                }
                RecordType::First => {
                    if self.fragmented {
                        return self.hard_error("first fragment found inside fragmented record");
                    }
                    self.fragmented = true;
                    self.record_position = self.position - physical_len as u64;
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
                    return Some(Ok(ScannedRecord {
                        position: self.record_position,
                        payload: Bytes::from(std::mem::take(&mut self.fragments)),
                    }));
                }
            }
        }
    }
}

#[cfg(test)]
fn scan_segment(path: &Path, tolerate_tail: bool) -> Result<SegmentScan, SegmentError> {
    let scanner = SegmentScanner::open(path, tolerate_tail, IoMode::Standard)?;
    scan0(scanner)
}

pub(crate) fn scan_file(
    file: Arc<VfsFile>,
    tolerate_tail: bool,
) -> Result<SegmentScan, SegmentError> {
    let scanner = SegmentScanner::open0(file, tolerate_tail)?;
    scan0(scanner)
}

fn scan0(mut scanner: SegmentScanner) -> Result<SegmentScan, SegmentError> {
    let segment_number = scanner.segment_number();
    let index = scanner
        .by_ref()
        .map(|record| record.map(|record| record.position))
        .collect::<Result<Vec<_>, _>>()?;
    let valid_len = scanner
        .valid_len()
        .checked_next_multiple_of(ALIGNMENT as u64)
        .ok_or(SegmentError::OffsetExhausted)?;
    Ok(SegmentScan {
        segment_number,
        index,
        valid_len,
    })
}

pub(crate) fn read_record(
    file: &Arc<VfsFile>,
    segment_number: u64,
    position: u64,
    end: u64,
) -> Result<Bytes, SegmentError> {
    let mut scanner = SegmentScanner::range(Arc::clone(file), segment_number, position, end)?;
    match scanner.next() {
        Some(Ok(record)) if record.position == position => Ok(record.payload),
        Some(Ok(_)) => corruption(file.path(), "segment index points inside a record"),
        Some(Err(error)) => Err(error),
        None => corruption(file.path(), "segment index points to no record"),
    }
}

fn decode_footer(bytes: &[u8]) -> Option<SegmentFooter> {
    if bytes.len() != FOOTER_SIZE || &bytes[..8] != FOOTER_MAGIC {
        return None;
    }
    let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    let fields_size = u16::from_le_bytes(bytes[10..12].try_into().unwrap()) as usize;
    if version != FOOTER_VERSION || fields_size != FOOTER_FIELDS_SIZE {
        return None;
    }
    let expected_checksum = u32::from_le_bytes(bytes[48..52].try_into().unwrap());
    if crc32c::crc32c(&bytes[..48]) != expected_checksum || !all_zero(&bytes[52..]) {
        return None;
    }
    Some(SegmentFooter {
        segment_number: u64::from_le_bytes(bytes[12..20].try_into().unwrap()),
        records_size: u64::from_le_bytes(bytes[20..28].try_into().unwrap()),
        record_count: u64::from_le_bytes(bytes[28..36].try_into().unwrap()),
        index_size: u64::from_le_bytes(bytes[36..44].try_into().unwrap()),
        index_checksum: u32::from_le_bytes(bytes[44..48].try_into().unwrap()),
    })
}

fn valid_index(index: &[u64], records_end: u64) -> bool {
    if index.is_empty() {
        return records_end == FILE_HEADER_SIZE as u64;
    }
    if let Some(first) = index.first()
        && *first != FILE_HEADER_SIZE as u64
    {
        return false;
    }
    index.iter().enumerate().all(|(position, value)| {
        *value >= FILE_HEADER_SIZE as u64
            && *value < records_end
            && value.is_multiple_of(ALIGNMENT as u64)
            && position
                .checked_sub(1)
                .is_none_or(|previous| index[previous] < *value)
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

fn align_up(value: usize, alignment: usize) -> usize {
    value.div_ceil(alignment) * alignment
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_records(records: &[Bytes]) -> Vec<u8> {
        let mut encoded = Vec::new();
        for record in records {
            let position = FILE_HEADER_SIZE as u64 + encoded.len() as u64;
            encoded.extend_from_slice(&encode_record(1, position, record).unwrap());
        }
        encoded
    }

    fn scan_records(path: &Path, tolerate_tail: bool) -> Result<Vec<Bytes>, SegmentError> {
        SegmentScanner::open(path, tolerate_tail, IoMode::Standard)?
            .map(|record| record.map(|record| record.payload))
            .collect()
    }

    #[test]
    fn header_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0000000007.seg");
        std::fs::write(&path, encode_file_header(7)).unwrap();
        let scan = scan_segment(&path, false).unwrap();
        assert_eq!(scan.segment_number, 7);
        assert!(scan.index.is_empty());
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

        assert_eq!(scan_records(&path, false).unwrap(), records);
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
        assert!(scan.index.is_empty());
        assert_eq!(scan.valid_len, FILE_HEADER_SIZE as u64);
        assert!(scan_segment(&path, false).is_err());
    }
}
