//! Active buffered WAL segment implementation.

use super::codec::{decode_record, encode_record};
use super::files::segment_path;
use super::{AppendResult, Segment, SegmentError, SegmentFile, SegmentOffset};
use bytes::Bytes;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug)]
pub(in crate::wal) struct FileSegment {
    // Immutable state
    number: u64,
    file: Arc<SegmentFile>,
    max_size: u64,

    // Mutable state
    write_position: u64,
}

impl FileSegment {
    pub(in crate::wal) fn create(
        dir: &Path,
        number: u64,
        max_size: u64,
    ) -> Result<Self, SegmentError> {
        u32::try_from(number).map_err(|_| SegmentError::SegmentNumberTooLarge(number))?;
        let file = Arc::new(SegmentFile::create(&segment_path(dir, number))?);
        Ok(Self {
            // Immutable state
            number,
            file,
            max_size,

            // Mutable state
            write_position: 0,
        })
    }

    pub(in crate::wal) fn open(
        file: Arc<SegmentFile>,
        number: u64,
        max_size: u64,
        write_position: u64,
    ) -> Result<Self, SegmentError> {
        u32::try_from(number).map_err(|_| SegmentError::SegmentNumberTooLarge(number))?;
        if write_position > max_size || file.size()? != write_position {
            return Err(SegmentError::Io(
                "recovered WAL segment position is inconsistent".into(),
            ));
        }
        Ok(Self {
            // Immutable state
            number,
            file,
            max_size,

            // Mutable state
            write_position,
        })
    }

    pub(in crate::wal) fn file(&self) -> Arc<SegmentFile> {
        Arc::clone(&self.file)
    }

    pub(in crate::wal) const fn write_position(&self) -> u64 {
        self.write_position
    }
}

impl Segment for FileSegment {
    fn read<R: Read>(
        &self,
        reader: &mut R,
        position: u64,
        file_end: u64,
    ) -> Result<(u64, Bytes), SegmentError> {
        decode_record(reader, self.file.path(), self.number, position, file_end)
    }

    fn append(&mut self, payload: &[u8]) -> Result<AppendResult, SegmentError> {
        let encoded = encode_record(self.number, self.write_position, payload)?;
        let encoded_size =
            u64::try_from(encoded.len()).map_err(|_| SegmentError::OffsetExhausted)?;
        let next_position = self
            .write_position
            .checked_add(encoded_size)
            .ok_or(SegmentError::OffsetExhausted)?;
        if next_position > self.max_size {
            if self.write_position == 0 {
                return Err(SegmentError::RecordTooLarge {
                    size: encoded_size,
                    max: self.max_size,
                });
            }
            return Ok(AppendResult::Full);
        }

        let offset = SegmentOffset::new(self.number, self.write_position);
        self.file.append(&encoded)?;
        self.write_position = next_position;
        Ok(AppendResult::Appended(offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Seek, SeekFrom};

    #[test]
    fn appends_and_reads_records_without_metadata_envelopes() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment = FileSegment::create(dir.path(), 1, 4096).unwrap();
        assert_eq!(
            segment.append(b"first").unwrap(),
            AppendResult::Appended(SegmentOffset::new(1, 0))
        );
        assert_eq!(
            segment.append(b"second").unwrap(),
            AppendResult::Appended(SegmentOffset::new(1, 16))
        );
        segment.file().sync(segment.write_position()).unwrap();

        let file = segment.file();
        let mut reader = BufReader::new(file.reader().unwrap());
        reader.seek(SeekFrom::Start(0)).unwrap();
        let (position, first) = segment
            .read(&mut reader, 0, segment.write_position())
            .unwrap();
        let (position, second) = segment
            .read(&mut reader, position, segment.write_position())
            .unwrap();
        assert_eq!(first, Bytes::from_static(b"first"));
        assert_eq!(second, Bytes::from_static(b"second"));
        assert_eq!(position, 33);
    }

    #[test]
    fn maximum_size_has_no_external_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment = FileSegment::create(dir.path(), 1, 16).unwrap();
        assert!(matches!(
            segment.append(b"first").unwrap(),
            AppendResult::Appended(_)
        ));
        assert_eq!(segment.append(b"second").unwrap(), AppendResult::Full);
        assert_eq!(segment.file().size().unwrap(), 16);
    }
}
