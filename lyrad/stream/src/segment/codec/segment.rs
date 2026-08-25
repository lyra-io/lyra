//! Segment header, index, and footer encoding and decoding.

use super::super::SegmentError;
use super::super::vfs::{IoFile, VfsFile};
use super::crc::checksum;
use super::record::BLOCK_SIZE;
use std::path::Path;

pub(crate) const ALIGNMENT: usize = 4096;
pub(crate) const FILE_HEADER_SIZE: usize = ALIGNMENT;
const FILE_MAGIC: &[u8; 8] = b"LYRASEG\0";
const FILE_VERSION: u16 = 3;
const FILE_HEADER_FIELDS_SIZE: usize = 32;
const FOOTER_MAGIC: &[u8; 8] = b"LYRAIDX\0";
const FOOTER_VERSION: u16 = 1;
const FOOTER_FIELDS_SIZE: usize = 52;
const FOOTER_SIZE: usize = ALIGNMENT;
const INDEX_ENTRY_SIZE: usize = size_of::<u64>();
pub(crate) struct SegmentHeader {
    pub(crate) segment_number: u64,
    pub(crate) alignment: usize,
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

pub(crate) fn encode_file_header(
    segment_number: u64,
    alignment: usize,
) -> Result<Vec<u8>, SegmentError> {
    validate_alignment(alignment)?;
    let alignment = u32::try_from(alignment).map_err(|_| SegmentError::OffsetExhausted)?;
    let mut header = vec![0; FILE_HEADER_SIZE];
    header[0..8].copy_from_slice(FILE_MAGIC);
    header[8..10].copy_from_slice(&FILE_VERSION.to_le_bytes());
    header[10..12].copy_from_slice(&(FILE_HEADER_FIELDS_SIZE as u16).to_le_bytes());
    header[12..20].copy_from_slice(&segment_number.to_le_bytes());
    header[20..24].copy_from_slice(&(BLOCK_SIZE as u32).to_le_bytes());
    header[24..28].copy_from_slice(&alignment.to_le_bytes());
    let checksum = checksum(&header[..28]);
    header[28..32].copy_from_slice(&checksum.to_le_bytes());
    Ok(header)
}

pub(crate) fn encode_index_footer(
    segment_number: u64,
    records_size: u64,
    index: &[u64],
    alignment: usize,
) -> Result<Vec<u8>, SegmentError> {
    validate_alignment(alignment)?;
    let entries_size = index
        .len()
        .checked_mul(INDEX_ENTRY_SIZE)
        .ok_or(SegmentError::OffsetExhausted)?;
    let index_size = if entries_size == 0 {
        0
    } else {
        align_up(entries_size, alignment)
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
    footer[44..48].copy_from_slice(&checksum(index_bytes).to_le_bytes());
    let footer_checksum = checksum(&footer[..48]);
    footer[48..52].copy_from_slice(&footer_checksum.to_le_bytes());
    Ok(output)
}

pub(crate) fn load_index(
    file: &VfsFile,
    alignment: usize,
) -> Result<Option<LoadedIndex>, SegmentError> {
    validate_alignment(alignment)?;
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
            .checked_next_multiple_of(alignment as u64)
            .ok_or_else(|| corruption_error(file.path(), "aligned segment index size overflows"))?
    };
    if footer.index_size != expected_index_size {
        return Ok(None);
    }

    let Some(index_start) = footer_start.checked_sub(footer.index_size) else {
        return Ok(None);
    };
    if !footer.records_size.is_multiple_of(alignment as u64) {
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
    if checksum(&index_bytes) != footer.index_checksum {
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
    if !valid_index(&index, index_start, alignment) {
        return Ok(None);
    }

    Ok(Some(LoadedIndex {
        segment_number: footer.segment_number,
        records_size: footer.records_size,
        index,
    }))
}

pub(crate) fn read_file_header(file: &VfsFile) -> Result<SegmentHeader, SegmentError> {
    if file.size()? < FILE_HEADER_SIZE as u64 {
        return corruption(file.path(), "truncated segment file header");
    }
    let header = file.read_at(0, FILE_HEADER_SIZE)?;
    decode_file_header(file.path(), &header)
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
    if checksum(&bytes[..48]) != expected_checksum || !all_zero(&bytes[52..]) {
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

fn valid_index(index: &[u64], records_end: u64, alignment: usize) -> bool {
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
            && value.is_multiple_of(alignment as u64)
            && position
                .checked_sub(1)
                .is_none_or(|previous| index[previous] < *value)
    })
}

pub(super) fn decode_file_header(path: &Path, bytes: &[u8]) -> Result<SegmentHeader, SegmentError> {
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
    if block_size != BLOCK_SIZE || validate_alignment(alignment).is_err() {
        return corruption(path, "unsupported segment block size or alignment");
    }
    let expected_checksum = u32::from_le_bytes(bytes[28..32].try_into().unwrap());
    let actual_checksum = checksum(&bytes[..28]);
    if actual_checksum != expected_checksum {
        return corruption(path, "segment header checksum mismatch");
    }
    Ok(SegmentHeader {
        segment_number: u64::from_le_bytes(bytes[12..20].try_into().unwrap()),
        alignment,
    })
}

pub(super) fn validate_alignment(alignment: usize) -> Result<(), SegmentError> {
    if alignment != 0 && FILE_HEADER_SIZE.is_multiple_of(alignment) {
        Ok(())
    } else {
        Err(SegmentError::Io(format!(
            "record alignment {alignment} must divide the {FILE_HEADER_SIZE}-byte segment envelope"
        )))
    }
}

pub(super) fn corruption<T>(path: &Path, message: &str) -> Result<T, SegmentError> {
    Err(corruption_error(path, message))
}

fn corruption_error(path: &Path, message: &str) -> SegmentError {
    SegmentError::Corruption {
        path: path.to_path_buf(),
        message: message.to_owned(),
    }
}

pub(super) fn all_zero(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| *byte == 0)
}

pub(super) fn align_up(value: usize, alignment: usize) -> usize {
    value.div_ceil(alignment) * alignment
}

#[cfg(test)]
mod tests {
    use super::super::super::IoMode;
    use super::super::super::vfs::open_local_file;
    use super::*;

    #[test]
    fn header_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("0000000007.seg");
        std::fs::write(&path, encode_file_header(7, ALIGNMENT).unwrap()).unwrap();

        let (_, file) = open_local_file(&path, IoMode::Standard).unwrap();
        let header = read_file_header(&file).unwrap();
        assert_eq!(header.segment_number, 7);
        assert_eq!(header.alignment, ALIGNMENT);
    }
}
