//! Buffered WAL segment implementation.

use super::codec::{decode_record, encode_record_parts};
use super::seg_reader::SegmentReader;
use super::{Segment, WalError, make_segment_path};
use crate::vfs::{IoFile, OpenOptions, Vfs, VfsFile, VfsI};
use bytes::Bytes;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// State shared by all handles to one segment.
#[derive(Debug)]
struct SegmentInner {
    // Immutable state
    number: u64,
    file: VfsFile,
    max_size: u64,

    // Mutable state
    write_position: AtomicU64,
}

impl SegmentInner {
    fn create(vfs: &VfsI, path: &Path, number: u64, max_size: u64) -> Result<Self, WalError> {
        Self::open0(vfs, path, number, max_size, 0, true)
    }

    fn open(
        vfs: &VfsI,
        path: &Path,
        number: u64,
        max_size: u64,
        write_position: u64,
    ) -> Result<Self, WalError> {
        Self::open0(vfs, path, number, max_size, write_position, false)
    }

    fn open0(
        vfs: &VfsI,
        path: &Path,
        number: u64,
        max_size: u64,
        write_position: u64,
        create_new: bool,
    ) -> Result<Self, WalError> {
        let options = if create_new {
            OpenOptions::CreateNew
        } else {
            OpenOptions::Existing
        };
        let file = vfs.open(path, options)?;
        Ok(Self {
            // Immutable state
            number,
            file,
            max_size,

            // Mutable state
            write_position: AtomicU64::new(write_position),
        })
    }

    fn path(&self) -> &Path {
        self.file.path()
    }

    fn size(&self) -> Result<u64, WalError> {
        Ok(self.file.size()?)
    }
}

/// A cheap, cloneable handle to one buffered WAL segment.
#[derive(Debug, Clone)]
pub(in crate::wal) struct FileSegment {
    // Immutable state
    inner: Arc<SegmentInner>,
}

/// A cheap, cloneable capability for synchronizing one WAL segment.
#[derive(Debug, Clone)]
pub(in crate::wal) struct SegmentSyncHandle {
    // Immutable state
    inner: Arc<SegmentInner>,
}

impl PartialEq for SegmentSyncHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Eq for SegmentSyncHandle {}

impl FileSegment {
    pub(in crate::wal) fn create(
        vfs: &VfsI,
        dir: &Path,
        number: u64,
        max_size: u64,
    ) -> Result<Self, WalError> {
        u32::try_from(number).map_err(|_| WalError::SegmentNumberTooLarge(number))?;
        let inner = Arc::new(SegmentInner::create(
            vfs,
            &make_segment_path(dir, number),
            number,
            max_size,
        )?);
        Ok(Self {
            // Immutable state
            inner,
        })
    }

    pub(in crate::wal) fn open(
        vfs: &VfsI,
        path: &Path,
        number: u64,
        max_size: u64,
        write_position: u64,
    ) -> Result<Self, WalError> {
        u32::try_from(number).map_err(|_| WalError::SegmentNumberTooLarge(number))?;
        let inner = Arc::new(SegmentInner::open(
            vfs,
            path,
            number,
            max_size,
            write_position,
        )?);
        if write_position > max_size || inner.size()? != write_position {
            return Err(WalError::corruption(
                inner.path(),
                "recovered WAL segment position is inconsistent",
            ));
        }
        Ok(Self {
            // Immutable state
            inner,
        })
    }

    pub(in crate::wal) fn sync_handle(&self) -> SegmentSyncHandle {
        SegmentSyncHandle {
            // Immutable state
            inner: Arc::clone(&self.inner),
        }
    }

    /// Appends as many complete records as fit using one file write and
    /// returns the number accepted before the segment boundary.
    pub(in crate::wal) fn append_batch<'a>(
        &self,
        records: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
    ) -> Result<usize, WalError> {
        let write_position = self.inner.write_position.load(Ordering::Relaxed);
        let mut next_position = write_position;
        let mut encoded = Vec::new();
        let mut appended = 0;

        for (prefix, payload) in records {
            let encoded_start = encoded.len();
            encode_record_parts(
                &mut encoded,
                self.inner.number,
                next_position,
                prefix,
                payload,
            )?;
            let encoded_size = u64::try_from(encoded.len() - encoded_start)
                .map_err(|_| WalError::PositionExhausted)?;
            let record_end = next_position
                .checked_add(encoded_size)
                .ok_or(WalError::PositionExhausted)?;
            if record_end > self.inner.max_size {
                encoded.truncate(encoded_start);
                if appended == 0 {
                    if write_position == 0 {
                        return Err(WalError::RecordTooLarge {
                            size: encoded_size,
                            max: self.inner.max_size,
                        });
                    }
                    return Err(WalError::SegmentFull);
                }
                break;
            }
            next_position = record_end;
            appended += 1;
        }

        if appended == 0 {
            return Ok(0);
        }
        let appended_at = self.inner.file.append(&encoded)?;
        if appended_at != write_position {
            return Err(WalError::corruption(
                self.inner.path(),
                format!(
                    "segment {} appended at {appended_at}, expected {write_position}",
                    self.inner.number
                ),
            ));
        }
        // Advance only after the entire encoded batch has been accepted.
        self.inner
            .write_position
            .store(next_position, Ordering::Release);
        Ok(appended)
    }
}

impl SegmentSyncHandle {
    pub(in crate::wal) fn sync(&self) -> Result<(), WalError> {
        // Capture the eviction boundary before syncing so a concurrent later
        // append cannot make us evict bytes that this call may not have synced.
        let write_position = self.inner.write_position.load(Ordering::Acquire);
        self.inner.file.sync()?;
        self.inner.file.discard_cache(write_position);
        Ok(())
    }
}

impl Segment for FileSegment {
    /// Reads complete records whose combined payload size does not exceed
    /// `max_bytes`, returning the next unread position and the payloads.
    fn read(&self, position: u64, max_bytes: usize) -> Result<(u64, Vec<Bytes>), WalError> {
        let write_position = self.inner.write_position.load(Ordering::Acquire);
        if position > write_position {
            return Err(WalError::corruption(
                self.inner.path(),
                format!(
                    "position {position} is outside segment {} with size {}",
                    self.inner.number, write_position
                ),
            ));
        }

        let mut reader = BufReader::new(SegmentReader {
            file: &self.inner.file,
            position,
            end: write_position,
        });
        let mut position = position;
        let mut payload_bytes = 0usize;
        let mut records = Vec::new();
        while position < write_position {
            let decoded = decode_record(
                &mut reader,
                self.inner.path(),
                self.inner.number,
                position,
                write_position,
            );
            let (next_position, payload) = match decoded {
                Ok(decoded) => decoded,
                // Return already decoded records first. A subsequent read from
                // this position will surface the boundary error.
                Err(_) if !records.is_empty() => break,
                Err(error) => return Err(error),
            };
            let next_payload_bytes = payload_bytes
                .checked_add(payload.len())
                .ok_or(WalError::PositionExhausted)?;
            if next_payload_bytes > max_bytes {
                if records.is_empty() {
                    return Err(WalError::ReadBufferTooSmall {
                        size: payload.len(),
                        max: max_bytes,
                    });
                }
                break;
            }

            records.push(payload);
            payload_bytes = next_payload_bytes;
            position = next_position;
        }

        self.inner.file.discard_cache(position);
        Ok((position, records))
    }

    fn append(&self, payload: &[u8]) -> Result<(), WalError> {
        let appended = self.append_batch(std::iter::once((&[][..], payload)))?;
        debug_assert_eq!(appended, 1);
        Ok(())
    }

    fn truncate(&self, position: u64) -> Result<(), WalError> {
        let write_position = self.inner.write_position.load(Ordering::Acquire);
        if position > write_position {
            return Err(WalError::corruption(
                self.inner.path(),
                format!(
                    "cannot truncate segment {} from {} to {position}",
                    self.inner.number, write_position
                ),
            ));
        }
        self.inner.file.truncate(position)?;
        self.inner.write_position.store(position, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::{MemoryVfs, StandardVfs, VfsI};

    #[test]
    fn appends_and_reads_records_without_metadata_envelopes() {
        let dir = tempfile::tempdir().unwrap();
        let vfs = VfsI::Standard(StandardVfs);
        let segment = FileSegment::create(&vfs, dir.path(), 1, 4096).unwrap();
        segment.append(b"first").unwrap();
        segment.append(b"second").unwrap();
        segment.sync_handle().sync().unwrap();

        let (next_position, records) = segment.read(0, 5).unwrap();
        assert_eq!(records, vec![Bytes::from_static(b"first")]);
        assert_eq!(next_position, 16);

        let (next_position, records) = segment.read(next_position, 6).unwrap();
        assert_eq!(records, vec![Bytes::from_static(b"second")]);
        assert_eq!(next_position, 33);
    }

    #[test]
    fn appends_multiple_records_with_one_batch() {
        let vfs = VfsI::Memory(MemoryVfs::default());
        let dir = Path::new("/wal");
        vfs.create_dir(dir).unwrap();
        let segment = FileSegment::create(&vfs, dir, 1, 4096).unwrap();
        let records = [(&b"pre"[..], &b"fix"[..]), (&b""[..], &b"second"[..])];

        assert_eq!(segment.append_batch(records), Ok(2));
        let (_, records) = segment.read(0, 12).unwrap();
        assert_eq!(
            records,
            vec![Bytes::from_static(b"prefix"), Bytes::from_static(b"second")]
        );
    }

    #[test]
    fn batch_stops_before_the_segment_limit() {
        let vfs = VfsI::Memory(MemoryVfs::default());
        let dir = Path::new("/wal");
        vfs.create_dir(dir).unwrap();
        let segment = FileSegment::create(&vfs, dir, 1, 16).unwrap();
        let records = [(&b""[..], &b"first"[..]), (&b""[..], &b"second"[..])];

        assert_eq!(segment.append_batch(records), Ok(1));
        assert_eq!(segment.append(b"second"), Err(WalError::SegmentFull));
        let (_, records) = segment.read(0, 5).unwrap();
        assert_eq!(records, vec![Bytes::from_static(b"first")]);
    }

    #[test]
    fn clones_observe_the_shared_append_position() {
        let vfs = VfsI::Memory(MemoryVfs::default());
        let dir = Path::new("/wal");
        vfs.create_dir(dir).unwrap();
        let cloned_vfs = vfs.clone();
        let segment = FileSegment::create(&cloned_vfs, dir, 1, 4096).unwrap();
        let cloned = segment.clone();

        segment.append(b"record").unwrap();

        let (_, records) = cloned.read(0, 6).unwrap();
        assert_eq!(records, vec![Bytes::from_static(b"record")]);
    }

    #[test]
    fn maximum_size_has_no_external_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let vfs = VfsI::Standard(StandardVfs);
        let segment = FileSegment::create(&vfs, dir.path(), 1, 16).unwrap();
        segment.append(b"first").unwrap();
        assert_eq!(segment.append(b"second"), Err(WalError::SegmentFull));
        assert_eq!(
            std::fs::metadata(make_segment_path(dir.path(), 1))
                .unwrap()
                .len(),
            16
        );
    }

    #[test]
    fn read_rejects_a_first_record_larger_than_the_byte_limit() {
        let dir = tempfile::tempdir().unwrap();
        let vfs = VfsI::Standard(StandardVfs);
        let segment = FileSegment::create(&vfs, dir.path(), 1, 4096).unwrap();
        segment.append(b"record").unwrap();

        assert!(matches!(
            segment.read(0, 5),
            Err(WalError::ReadBufferTooSmall { size: 6, max: 5 })
        ));
    }
}
