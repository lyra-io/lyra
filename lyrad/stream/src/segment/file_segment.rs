//! File-backed record-level segment implementation.

use super::files::segment_path;
use super::format::{
    FILE_HEADER_SIZE, encode_file_header, encode_index_footer, encode_record, load_index,
    read_file_header, read_record, scan_file,
};
use super::vfs::{IoFile, OpenOptions, Vfs, VfsFile, create_local_file, open_local_file};
use super::{AppendResult, IoMode, Segment, SegmentError, SegmentOffset};
use bytes::Bytes;
use std::io::ErrorKind;
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
    file: Arc<VfsFile>,
    max_records_size: u64,

    // Mutable state
    records_size: u64,
    append_position: SegmentOffset,
    index: Vec<u64>,
    state: SegmentState,
}

impl FileSegment {
    pub fn create(
        vfs: &dyn Vfs,
        dir: &Path,
        number: u64,
        max_records_size: u64,
    ) -> Result<Self, SegmentError> {
        let path = segment_path(dir, number);
        let file = vfs.open(&path, OpenOptions::CreateNew)?;
        if let Err(error) = file.write_at(0, &encode_file_header(number)) {
            drop(file);
            match vfs.remove(&path) {
                Ok(()) => {}
                Err(cleanup_error) if cleanup_error.kind() == ErrorKind::NotFound => {}
                Err(cleanup_error) => return Err(cleanup_error.into()),
            }
            return Err(error.into());
        }
        Ok(Self::create0(file, number, max_records_size))
    }

    pub(crate) fn create_local(
        dir: &Path,
        number: u64,
        max_records_size: u64,
        io_mode: IoMode,
    ) -> Result<Self, SegmentError> {
        let path = segment_path(dir, number);
        let file = create_local_file(&path, io_mode)?;
        if let Err(error) = file.write_at(0, &encode_file_header(number)) {
            drop(file);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(cleanup_error) if cleanup_error.kind() == ErrorKind::NotFound => {}
                Err(cleanup_error) => return Err(cleanup_error.into()),
            }
            return Err(error.into());
        }
        Ok(Self::create0(file, number, max_records_size))
    }

    fn create0(file: VfsFile, number: u64, max_records_size: u64) -> Self {
        Self {
            // Immutable state
            number,
            file: Arc::new(file),
            max_records_size,

            // Mutable state
            records_size: 0,
            append_position: SegmentOffset::new(0),
            index: Vec::new(),
            state: SegmentState::Active,
        }
    }

    pub fn open(
        vfs: &dyn Vfs,
        path: impl AsRef<Path>,
        max_records_size: u64,
    ) -> Result<Self, SegmentError> {
        let path = path.as_ref();
        let file = Arc::new(vfs.open(path, OpenOptions::Existing)?);
        Self::open0(path, file, max_records_size)
    }

    pub(crate) fn open_local(
        path: impl AsRef<Path>,
        max_records_size: u64,
        io_mode: IoMode,
    ) -> Result<Self, SegmentError> {
        let path = path.as_ref();
        let file = Arc::new(open_local_file(path, io_mode)?);
        Self::open0(path, file, max_records_size)
    }

    fn open0(path: &Path, file: Arc<VfsFile>, max_records_size: u64) -> Result<Self, SegmentError> {
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

    pub(crate) fn file(&self) -> Arc<VfsFile> {
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

        self.file.write_at(position, &encoded)?;
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
        self.file.truncate(position)?;
        self.file.write_at(position, &encoded)?;
        self.state = SegmentState::Sealed;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::format::{ALIGNMENT, PHYSICAL_HEADER_SIZE};
    use super::super::vfs::MemoryVfs;
    use super::*;

    #[test]
    fn memory_vfs_seals_and_reopens_a_segment() {
        let vfs = MemoryVfs::default();
        let dir = Path::new("/wal");
        vfs.create_dir(dir).unwrap();
        let mut segment = FileSegment::create(&vfs, dir, 1, ALIGNMENT as u64 * 2).unwrap();

        assert_eq!(
            segment.append(b"first").unwrap(),
            AppendResult::Appended(SegmentOffset::new(0))
        );
        assert_eq!(
            segment.append(b"second").unwrap(),
            AppendResult::Appended(SegmentOffset::new(1))
        );
        segment.seal().unwrap();

        let path = segment.file.path().to_path_buf();
        let reopened = FileSegment::open(&vfs, path, ALIGNMENT as u64 * 2).unwrap();
        assert_eq!(
            reopened.read(SegmentOffset::new(0)).unwrap(),
            Some(Bytes::from_static(b"first"))
        );
        assert_eq!(
            reopened.read(SegmentOffset::new(1)).unwrap(),
            Some(Bytes::from_static(b"second"))
        );
    }

    #[test]
    fn sealed_segment_reads_payloads_by_offset() {
        for io_mode in [IoMode::Standard, IoMode::DirectPreferred] {
            let dir = tempfile::tempdir().unwrap();
            let mut segment =
                FileSegment::create_local(dir.path(), 1, ALIGNMENT as u64 * 2, io_mode).unwrap();

            assert_eq!(
                segment.append(b"first").unwrap(),
                AppendResult::Appended(SegmentOffset::new(0))
            );
            assert_eq!(
                segment.append(b"second").unwrap(),
                AppendResult::Appended(SegmentOffset::new(1))
            );
            segment.seal().unwrap();
            segment.file.sync().unwrap();

            let reopened =
                FileSegment::open_local(segment.file.path(), ALIGNMENT as u64 * 2, io_mode)
                    .unwrap();
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
            FileSegment::create_local(dir.path(), 1, ALIGNMENT as u64, IoMode::Standard).unwrap();
        assert!(matches!(
            segment.append(b"record").unwrap(),
            AppendResult::Appended(_)
        ));
        assert_eq!(segment.append(b"full").unwrap(), AppendResult::Full);
        segment.seal().unwrap();

        assert!(segment.file.size().unwrap() > ALIGNMENT as u64);
    }

    #[test]
    fn valid_footer_opens_without_reading_record_data() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment =
            FileSegment::create_local(dir.path(), 1, ALIGNMENT as u64, IoMode::Standard).unwrap();
        segment.append(b"record").unwrap();
        segment.seal().unwrap();
        segment.file.sync().unwrap();
        let path = segment.file.path().to_path_buf();

        let mut bytes = std::fs::read(&path).unwrap();
        bytes[FILE_HEADER_SIZE + PHYSICAL_HEADER_SIZE] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();

        let reopened = FileSegment::open_local(&path, ALIGNMENT as u64, IoMode::Standard).unwrap();
        assert!(reopened.read(SegmentOffset::new(0)).is_err());
    }

    #[test]
    fn corrupt_footer_rebuilds_and_rewrites_the_index() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment =
            FileSegment::create_local(dir.path(), 1, ALIGNMENT as u64, IoMode::Standard).unwrap();
        segment.append(b"record").unwrap();
        segment.seal().unwrap();
        segment.file.sync().unwrap();
        let path = segment.file.path().to_path_buf();

        let file_len = std::fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(file_len - 1)
            .unwrap();

        let mut reopened =
            FileSegment::open_local(&path, ALIGNMENT as u64, IoMode::Standard).unwrap();
        assert!(reopened.needs_repair());
        assert_eq!(
            reopened.read(SegmentOffset::new(0)).unwrap(),
            Some(Bytes::from_static(b"record"))
        );
        reopened.seal().unwrap();
        reopened.file.sync().unwrap();
        drop(reopened);

        let reopened = FileSegment::open_local(&path, ALIGNMENT as u64, IoMode::Standard).unwrap();
        assert_eq!(reopened.record_count(), 1);
    }
}
