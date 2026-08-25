//! File-backed record-level segment implementation.

use super::codec::{
    ALIGNMENT, FILE_HEADER_SIZE, encode_file_header, encode_index_footer, encode_record,
    load_index, read_file_header, read_record, scan_file,
};
use super::vfs::{IoFile, OpenOptions, Vfs, VfsFile, create_local_file, open_local_file};
use super::{AppendResult, IoMode, Segment, SegmentError, SegmentOffset, segment_path};
use bytes::Bytes;
use std::io::{Error, ErrorKind};
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
    vfs: Arc<dyn Vfs>,
    file: Arc<VfsFile>,
    max_records_size: u64,
    alignment: usize,

    // Mutable state
    record_index: Vec<u64>,
    state: SegmentState,
}

impl FileSegment {
    pub fn create(
        vfs: Arc<dyn Vfs>,
        dir: &Path,
        number: u64,
        max_records_size: u64,
    ) -> Result<Self, SegmentError> {
        let alignment = record_alignment(vfs.as_ref())?;
        let max_records_size = align_max_records_size(max_records_size, alignment)?;
        let path = segment_path(dir, number);
        let file = vfs.open(&path, OpenOptions::CreateNew)?;
        let append = file
            .append(&encode_file_header(number, alignment)?)
            .and_then(|position| {
                if position == 0 {
                    Ok(())
                } else {
                    Err(Error::other("new segment file is not empty"))
                }
            });
        if let Err(error) = append {
            drop(file);
            match vfs.remove(&path) {
                Ok(()) => {}
                Err(cleanup_error) if cleanup_error.kind() == ErrorKind::NotFound => {}
                Err(cleanup_error) => return Err(cleanup_error.into()),
            }
            return Err(error.into());
        }
        Ok(Self {
            // Immutable state
            number,
            vfs,
            file: Arc::new(file),
            max_records_size,
            alignment,

            // Mutable state
            record_index: vec![FILE_HEADER_SIZE as u64],
            state: SegmentState::Active,
        })
    }

    pub(crate) fn create_local(
        dir: &Path,
        number: u64,
        max_records_size: u64,
        io_mode: IoMode,
    ) -> Result<Self, SegmentError> {
        let path = segment_path(dir, number);
        let (vfs, file) = create_local_file(&path, io_mode)?;
        let (alignment, max_records_size) =
            match record_alignment(vfs.as_ref()).and_then(|alignment| {
                align_max_records_size(max_records_size, alignment)
                    .map(|max_records_size| (alignment, max_records_size))
            }) {
                Ok(configuration) => configuration,
                Err(error) => {
                    drop(file);
                    match std::fs::remove_file(&path) {
                        Ok(()) => {}
                        Err(cleanup_error) if cleanup_error.kind() == ErrorKind::NotFound => {}
                        Err(cleanup_error) => return Err(cleanup_error.into()),
                    }
                    return Err(error);
                }
            };
        let append = file
            .append(&encode_file_header(number, alignment)?)
            .and_then(|position| {
                if position == 0 {
                    Ok(())
                } else {
                    Err(Error::other("new segment file is not empty"))
                }
            });
        if let Err(error) = append {
            drop(file);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(cleanup_error) if cleanup_error.kind() == ErrorKind::NotFound => {}
                Err(cleanup_error) => return Err(cleanup_error.into()),
            }
            return Err(error.into());
        }
        Ok(Self {
            // Immutable state
            number,
            vfs,
            file: Arc::new(file),
            max_records_size,
            alignment,

            // Mutable state
            record_index: vec![FILE_HEADER_SIZE as u64],
            state: SegmentState::Active,
        })
    }

    pub fn open(
        vfs: Arc<dyn Vfs>,
        path: impl AsRef<Path>,
        max_records_size: u64,
    ) -> Result<Self, SegmentError> {
        let path = path.as_ref();
        let file = Arc::new(vfs.open(path, OpenOptions::Existing)?);
        Self::open0(path, vfs, file, max_records_size)
    }

    pub(crate) fn open_local(
        path: impl AsRef<Path>,
        max_records_size: u64,
        io_mode: IoMode,
    ) -> Result<Self, SegmentError> {
        let path = path.as_ref();
        let (vfs, file) = open_local_file(path, io_mode)?;
        Self::open0(path, vfs, Arc::new(file), max_records_size)
    }

    fn open0(
        path: &Path,
        vfs: Arc<dyn Vfs>,
        file: Arc<VfsFile>,
        max_records_size: u64,
    ) -> Result<Self, SegmentError> {
        let header = read_file_header(&file)?;
        let max_records_size = align_max_records_size(max_records_size, header.alignment)?;
        let required_alignment = record_alignment(vfs.as_ref())?;
        if !header.alignment.is_multiple_of(required_alignment) {
            return Err(SegmentError::Io(format!(
                "segment record alignment {} does not satisfy VFS alignment {required_alignment}",
                header.alignment,
            )));
        }
        if let Some(loaded) = load_index(&file, header.alignment)? {
            if header.segment_number != loaded.segment_number {
                return Err(SegmentError::Corruption {
                    path: path.to_path_buf(),
                    message: format!(
                        "segment header identifies {}, but its footer identifies {}",
                        header.segment_number, loaded.segment_number
                    ),
                });
            }
            let records_end = (FILE_HEADER_SIZE as u64)
                .checked_add(loaded.records_size)
                .ok_or(SegmentError::OffsetExhausted)?;
            let mut record_index = loaded.index;
            record_index.push(records_end);
            return Ok(Self {
                // Immutable state
                number: header.segment_number,
                vfs,
                file,
                max_records_size,
                alignment: header.alignment,

                // Mutable state
                record_index,
                state: SegmentState::Sealed,
            });
        }

        let scan = scan_file(Arc::clone(&file), true)?;
        scan.valid_len
            .checked_sub(FILE_HEADER_SIZE as u64)
            .ok_or_else(|| SegmentError::Corruption {
                path: path.to_path_buf(),
                message: "segment records end before the file header".into(),
            })?;
        let mut record_index = scan.index;
        record_index.push(scan.valid_len);
        Ok(Self {
            // Immutable state
            number: scan.segment_number,
            vfs,
            file,
            max_records_size,
            alignment: header.alignment,

            // Mutable state
            record_index,
            state: SegmentState::NeedsRepair,
        })
    }

    pub const fn number(&self) -> u64 {
        self.number
    }

    pub fn record_count(&self) -> u64 {
        self.record_index.len().saturating_sub(1) as u64
    }

    pub fn vfs(&self) -> &dyn Vfs {
        self.vfs.as_ref()
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

        let position = self
            .record_index
            .last()
            .copied()
            .ok_or_else(|| SegmentError::Io("segment record index is empty".into()))?;
        let encoded = encode_record(self.number, position, payload, self.alignment)?;
        let encoded_size =
            u64::try_from(encoded.len()).map_err(|_| SegmentError::OffsetExhausted)?;
        if encoded_size > self.max_records_size {
            return Err(SegmentError::RecordTooLarge {
                size: encoded_size,
                max: self.max_records_size,
            });
        }
        let next_position = position
            .checked_add(encoded_size)
            .ok_or(SegmentError::OffsetExhausted)?;
        let records_limit = (FILE_HEADER_SIZE as u64)
            .checked_add(self.max_records_size)
            .ok_or(SegmentError::OffsetExhausted)?;
        if next_position > records_limit {
            return Ok(AppendResult::Full);
        }

        let actual_position = self.file.append(&encoded)?;
        if actual_position != position {
            return Err(SegmentError::Io(format!(
                "segment append position is {actual_position}, expected {position}"
            )));
        }
        let offset = u64::try_from(self.record_index.len() - 1)
            .map(SegmentOffset::new)
            .map_err(|_| SegmentError::OffsetExhausted)?;
        self.record_index.push(next_position);
        Ok(AppendResult::Appended(offset))
    }

    fn read(&self, offset: SegmentOffset) -> Result<Option<Bytes>, SegmentError> {
        let Ok(index) = usize::try_from(offset.get()) else {
            return Ok(None);
        };
        let Some((position, end)) = self
            .record_index
            .get(index)
            .copied()
            .zip(self.record_index.get(index + 1).copied())
        else {
            return Ok(None);
        };
        read_record(&self.file, self.number, position, end, self.alignment).map(Some)
    }

    fn seal(&mut self) -> Result<(), SegmentError> {
        if self.state == SegmentState::Sealed {
            return Ok(());
        }

        let position = self
            .record_index
            .last()
            .copied()
            .ok_or_else(|| SegmentError::Io("segment record index is empty".into()))?;
        let records_size = position
            .checked_sub(FILE_HEADER_SIZE as u64)
            .ok_or(SegmentError::OffsetExhausted)?;
        let record_positions = &self.record_index[..self.record_index.len() - 1];
        let encoded =
            encode_index_footer(self.number, records_size, record_positions, self.alignment)?;
        self.file.truncate(position)?;
        let actual_position = self.file.append(&encoded)?;
        if actual_position != position {
            return Err(SegmentError::Io(format!(
                "segment tail position is {actual_position}, expected {position}"
            )));
        }
        self.state = SegmentState::Sealed;
        Ok(())
    }
}

fn record_alignment(vfs: &dyn Vfs) -> Result<usize, SegmentError> {
    let alignment = usize::try_from(vfs.alignment().unwrap_or(ALIGNMENT as u64))
        .map_err(|_| SegmentError::OffsetExhausted)?;
    if alignment != 0 && FILE_HEADER_SIZE.is_multiple_of(alignment) {
        Ok(alignment)
    } else {
        Err(SegmentError::Io(format!(
            "VFS alignment {alignment} must divide the {FILE_HEADER_SIZE}-byte segment envelope"
        )))
    }
}

fn align_max_records_size(max_records_size: u64, alignment: usize) -> Result<u64, SegmentError> {
    max_records_size
        .checked_next_multiple_of(alignment as u64)
        .ok_or(SegmentError::OffsetExhausted)
}

#[cfg(test)]
mod tests {
    use super::super::codec::{ALIGNMENT, PHYSICAL_HEADER_SIZE};
    use super::super::vfs::MemoryVfs;
    use super::*;

    #[test]
    fn memory_vfs_seals_and_reopens_a_segment() {
        let vfs: Arc<dyn Vfs> = Arc::new(MemoryVfs::default());
        let dir = Path::new("/wal");
        vfs.create_dir(dir).unwrap();
        let mut segment =
            FileSegment::create(Arc::clone(&vfs), dir, 1, ALIGNMENT as u64 * 2).unwrap();

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
        let reopened = FileSegment::open(vfs, path, ALIGNMENT as u64 * 2).unwrap();
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
    fn record_area_limit_rounds_up_to_record_alignment() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment =
            FileSegment::create_local(dir.path(), 1, ALIGNMENT as u64 + 1, IoMode::Standard)
                .unwrap();

        assert!(matches!(
            segment.append(b"first").unwrap(),
            AppendResult::Appended(_)
        ));
        assert!(matches!(
            segment.append(b"second").unwrap(),
            AppendResult::Appended(_)
        ));
        assert_eq!(segment.append(b"full").unwrap(), AppendResult::Full);
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
