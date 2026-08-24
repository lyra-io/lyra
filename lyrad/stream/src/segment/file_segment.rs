//! File-backed record-level segment implementation.

use super::format::{
    FILE_HEADER_SIZE, encode_index_footer, encode_record, load_index, read_file_header,
    read_record, scan_file,
};
use super::io::{AlignedBuffer, SegmentFile};
use super::{AppendResult, IoMode, Segment, SegmentError, SegmentOffset};
use bytes::Bytes;
use std::path::Path;
use std::sync::Arc;

impl SegmentOffset {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentState {
    Active,
    NeedsRepair,
    Sealed,
}

/// A file-backed segment with an in-memory record-position index.
pub struct FileSegment {
    // Immutable state
    number: u64,
    file: Arc<SegmentFile>,
    max_records_size: u64,

    // Mutable state
    records_size: u64,
    append_position: SegmentOffset,
    index: Vec<u64>,
    state: SegmentState,
}

impl FileSegment {
    pub fn create(
        dir: &Path,
        number: u64,
        max_records_size: u64,
        io_mode: IoMode,
    ) -> Result<Self, SegmentError> {
        let file = SegmentFile::create(dir, number, io_mode)?;
        Ok(Self {
            // Immutable state
            number,
            file,
            max_records_size,

            // Mutable state
            records_size: 0,
            append_position: SegmentOffset::new(0),
            index: Vec::new(),
            state: SegmentState::Active,
        })
    }

    pub fn open(
        path: impl AsRef<Path>,
        max_records_size: u64,
        io_mode: IoMode,
    ) -> Result<Self, SegmentError> {
        let path = path.as_ref();
        let file = Arc::new(SegmentFile::open(path, io_mode)?);

        if let Some(loaded) = load_index(&file)? {
            let header_number = read_file_header(&file)?;
            if header_number != loaded.segment_number {
                return Err(SegmentError::Corruption {
                    path: path.to_path_buf(),
                    message: format!(
                        "segment header identifies {header_number}, but its footer identifies {}",
                        loaded.segment_number
                    ),
                });
            }
            let append_position = u64::try_from(loaded.index.len())
                .map(SegmentOffset::new)
                .map_err(|_| SegmentError::OffsetExhausted)?;
            return Ok(Self {
                // Immutable state
                number: header_number,
                file,
                max_records_size,

                // Mutable state
                records_size: loaded.records_size,
                append_position,
                index: loaded.index,
                state: SegmentState::Sealed,
            });
        }

        let scan = scan_file(Arc::clone(&file), true)?;
        let records_size = scan
            .valid_len
            .checked_sub(FILE_HEADER_SIZE as u64)
            .ok_or_else(|| SegmentError::Corruption {
                path: path.to_path_buf(),
                message: "segment records end before the file header".into(),
            })?;
        let append_position = u64::try_from(scan.index.len())
            .map(SegmentOffset::new)
            .map_err(|_| SegmentError::OffsetExhausted)?;
        Ok(Self {
            // Immutable state
            number: scan.segment_number,
            file,
            max_records_size,

            // Mutable state
            records_size,
            append_position,
            index: scan.index,
            state: SegmentState::NeedsRepair,
        })
    }

    pub const fn number(&self) -> u64 {
        self.number
    }

    pub const fn record_count(&self) -> u64 {
        self.append_position.get()
    }

    pub(crate) fn file(&self) -> Arc<SegmentFile> {
        Arc::clone(&self.file)
    }

    pub(crate) fn needs_repair(&self) -> bool {
        self.state == SegmentState::NeedsRepair
    }
}

impl Segment for FileSegment {
    fn append(&mut self, payload: &[u8]) -> Result<AppendResult, SegmentError> {
        if self.state != SegmentState::Active {
            return Err(SegmentError::Sealed);
        }

        let position = (FILE_HEADER_SIZE as u64)
            .checked_add(self.records_size)
            .ok_or(SegmentError::OffsetExhausted)?;
        let encoded = encode_record(self.number, position, payload)?;
        let encoded_size =
            u64::try_from(encoded.len()).map_err(|_| SegmentError::OffsetExhausted)?;
        if encoded_size > self.max_records_size {
            return Err(SegmentError::RecordTooLarge {
                size: encoded_size,
                max: self.max_records_size,
            });
        }
        let next_records_size = self
            .records_size
            .checked_add(encoded_size)
            .ok_or(SegmentError::OffsetExhausted)?;
        if next_records_size > self.max_records_size {
            return Ok(AppendResult::Full);
        }

        let buffer = AlignedBuffer::from_slice(&encoded);
        self.file.write_aligned(&buffer, position)?;
        let offset = self.append_position;
        self.append_position = SegmentOffset::new(
            offset
                .get()
                .checked_add(1)
                .ok_or(SegmentError::OffsetExhausted)?,
        );
        self.records_size = next_records_size;
        self.index.push(position);
        Ok(AppendResult::Appended(offset))
    }

    fn read(&self, offset: SegmentOffset) -> Result<Option<Bytes>, SegmentError> {
        let Ok(index) = usize::try_from(offset.get()) else {
            return Ok(None);
        };
        let Some(position) = self.index.get(index).copied() else {
            return Ok(None);
        };
        let end = self
            .index
            .get(index + 1)
            .copied()
            .unwrap_or(FILE_HEADER_SIZE as u64 + self.records_size);
        read_record(&self.file, self.number, position, end).map(Some)
    }

    fn seal(&mut self) -> Result<(), SegmentError> {
        if self.state == SegmentState::Sealed {
            return Ok(());
        }

        let position = (FILE_HEADER_SIZE as u64)
            .checked_add(self.records_size)
            .ok_or(SegmentError::OffsetExhausted)?;
        let encoded = encode_index_footer(self.number, self.records_size, &self.index)?;
        let buffer = AlignedBuffer::from_slice(&encoded);
        self.file.set_len(position)?;
        self.file.write_aligned(&buffer, position)?;
        self.state = SegmentState::Sealed;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::format::{ALIGNMENT, PHYSICAL_HEADER_SIZE};
    use super::*;

    #[test]
    fn sealed_segment_reads_payloads_by_offset() {
        for io_mode in [IoMode::Standard, IoMode::DirectPreferred] {
            let dir = tempfile::tempdir().unwrap();
            let mut segment =
                FileSegment::create(dir.path(), 1, ALIGNMENT as u64 * 2, io_mode).unwrap();

            assert_eq!(
                segment.append(b"first").unwrap(),
                AppendResult::Appended(SegmentOffset::new(0))
            );
            assert_eq!(
                segment.append(b"second").unwrap(),
                AppendResult::Appended(SegmentOffset::new(1))
            );
            segment.seal().unwrap();
            segment.file.sync_data().unwrap();

            let reopened =
                FileSegment::open(segment.file.path(), ALIGNMENT as u64 * 2, io_mode).unwrap();
            assert_eq!(
                reopened.read(SegmentOffset::new(0)).unwrap(),
                Some(Bytes::from_static(b"first"))
            );
            assert_eq!(
                reopened.read(SegmentOffset::new(1)).unwrap(),
                Some(Bytes::from_static(b"second"))
            );
            assert_eq!(reopened.read(SegmentOffset::new(2)).unwrap(), None);
        }
    }

    #[test]
    fn record_area_limit_excludes_header_index_and_footer() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment =
            FileSegment::create(dir.path(), 1, ALIGNMENT as u64, IoMode::Standard).unwrap();
        assert!(matches!(
            segment.append(b"record").unwrap(),
            AppendResult::Appended(_)
        ));
        assert_eq!(segment.append(b"full").unwrap(), AppendResult::Full);
        segment.seal().unwrap();

        assert!(segment.file.len().unwrap() > ALIGNMENT as u64);
    }

    #[test]
    fn valid_footer_opens_without_reading_record_data() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment =
            FileSegment::create(dir.path(), 1, ALIGNMENT as u64, IoMode::Standard).unwrap();
        segment.append(b"record").unwrap();
        segment.seal().unwrap();
        segment.file.sync_data().unwrap();
        let path = segment.file.path().to_path_buf();

        let mut bytes = std::fs::read(&path).unwrap();
        bytes[FILE_HEADER_SIZE + PHYSICAL_HEADER_SIZE] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();

        let reopened = FileSegment::open(&path, ALIGNMENT as u64, IoMode::Standard).unwrap();
        assert!(reopened.read(SegmentOffset::new(0)).is_err());
    }

    #[test]
    fn corrupt_footer_rebuilds_and_rewrites_the_index() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment =
            FileSegment::create(dir.path(), 1, ALIGNMENT as u64, IoMode::Standard).unwrap();
        segment.append(b"record").unwrap();
        segment.seal().unwrap();
        segment.file.sync_data().unwrap();
        let path = segment.file.path().to_path_buf();

        let file_len = std::fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(file_len - 1)
            .unwrap();

        let mut reopened = FileSegment::open(&path, ALIGNMENT as u64, IoMode::Standard).unwrap();
        assert!(reopened.needs_repair());
        assert_eq!(
            reopened.read(SegmentOffset::new(0)).unwrap(),
            Some(Bytes::from_static(b"record"))
        );
        reopened.seal().unwrap();
        reopened.file.sync_data().unwrap();
        drop(reopened);

        let reopened = FileSegment::open(&path, ALIGNMENT as u64, IoMode::Standard).unwrap();
        assert_eq!(reopened.record_count(), 1);
    }
}
