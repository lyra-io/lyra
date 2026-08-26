//! Buffered WAL segment implementation.

use super::codec::{decode_record, encode_record};
use super::{AppendResult, Segment, WalError, make_segment_path};
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Result as IoResult, Seek, SeekFrom, Write};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub(in crate::wal) struct FileHandle {
    // Immutable state
    path: PathBuf,
    file: File,

    // Mutable state
    evicted_position: AtomicU64,
}

impl FileHandle {
    fn create(path: &Path) -> Result<Self, WalError> {
        Self::open0(path, true)
    }

    pub(in crate::wal) fn open(path: &Path) -> Result<Self, WalError> {
        Self::open0(path, false)
    }

    fn open0(path: &Path, create_new: bool) -> Result<Self, WalError> {
        let mut options = OpenOptions::new();
        options.read(true).append(true).create_new(create_new);
        let file = options.open(path)?;
        Ok(Self {
            // Immutable state
            path: path.to_path_buf(),
            file,

            // Mutable state
            evicted_position: AtomicU64::new(0),
        })
    }

    pub(in crate::wal) fn path(&self) -> &Path {
        &self.path
    }

    pub(in crate::wal) fn size(&self) -> Result<u64, WalError> {
        Ok(self.file.metadata()?.len())
    }

    fn reader(&self) -> Result<File, WalError> {
        Ok(self.file.try_clone()?)
    }

    fn append(&self, bytes: &[u8]) -> Result<(), WalError> {
        (&self.file).write_all(bytes)?;
        Ok(())
    }

    pub(in crate::wal) fn truncate(&self, size: u64) -> Result<(), WalError> {
        self.file.set_len(size)?;
        Ok(())
    }

    pub(in crate::wal) fn sync(&self, end: u64) -> IoResult<()> {
        self.file.sync_data()?;
        self.discard_cache(end);
        Ok(())
    }

    pub(in crate::wal) fn discard_cache(&self, end: u64) {
        self.discard_cache0(end);
    }

    #[cfg(target_os = "linux")]
    fn discard_cache0(&self, end: u64) {
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            tracing::warn!(path = %self.path.display(), "failed to determine page size for WAL cache eviction");
            return;
        }
        let page_size = page_size as u64;
        let aligned_end = end - end % page_size;
        let start = self.evicted_position.load(Ordering::Acquire);
        if aligned_end <= start {
            return;
        }

        let Ok(offset) = libc::off_t::try_from(start) else {
            tracing::warn!(path = %self.path.display(), start, "WAL cache eviction offset exceeds the platform limit");
            return;
        };
        let Ok(length) = libc::off_t::try_from(aligned_end - start) else {
            tracing::warn!(path = %self.path.display(), start, aligned_end, "WAL cache eviction length exceeds the platform limit");
            return;
        };
        let status = unsafe {
            libc::posix_fadvise(
                self.file.as_raw_fd(),
                offset,
                length,
                libc::POSIX_FADV_DONTNEED,
            )
        };
        if status == 0 {
            self.evicted_position.store(aligned_end, Ordering::Release);
        } else {
            tracing::warn!(
                path = %self.path.display(),
                start,
                aligned_end,
                error = %std::io::Error::from_raw_os_error(status),
                "failed to evict synced WAL pages from the page cache"
            );
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn discard_cache0(&self, _end: u64) {
        let _ = self.evicted_position.load(Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(in crate::wal) struct FileSegment {
    // Immutable state
    number: u64,
    file: Arc<FileHandle>,
    max_size: u64,

    // Mutable state
    write_position: u64,
}

impl FileSegment {
    pub(in crate::wal) fn create(dir: &Path, number: u64, max_size: u64) -> Result<Self, WalError> {
        u32::try_from(number).map_err(|_| WalError::SegmentNumberTooLarge(number))?;
        let file = Arc::new(FileHandle::create(&make_segment_path(dir, number))?);
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
        file: Arc<FileHandle>,
        number: u64,
        max_size: u64,
        write_position: u64,
    ) -> Result<Self, WalError> {
        u32::try_from(number).map_err(|_| WalError::SegmentNumberTooLarge(number))?;
        if write_position > max_size || file.size()? != write_position {
            return Err(WalError::corruption(
                file.path(),
                "recovered WAL segment position is inconsistent",
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
}

impl Segment for FileSegment {
    fn file(&self) -> Arc<FileHandle> {
        Arc::clone(&self.file)
    }

    fn write_position(&self) -> u64 {
        self.write_position
    }

    /// Reads complete records whose combined payload size does not exceed
    /// `max_bytes`, returning the next unread position and the payloads.
    fn read(&self, position: u64, max_bytes: usize) -> Result<(u64, Vec<Bytes>), WalError> {
        if position > self.write_position {
            return Err(WalError::corruption(
                self.file.path(),
                format!(
                    "position {position} is outside segment {} with size {}",
                    self.number, self.write_position
                ),
            ));
        }

        let mut reader = BufReader::new(self.file.reader()?);
        reader.seek(SeekFrom::Start(position))?;
        let mut position = position;
        let mut payload_bytes = 0usize;
        let mut records = Vec::new();
        while position < self.write_position {
            let decoded = decode_record(
                &mut reader,
                self.file.path(),
                self.number,
                position,
                self.write_position,
            );
            let (next_position, payload) = match decoded {
                Ok(decoded) => decoded,
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

        Ok((position, records))
    }

    fn append(&mut self, payload: &[u8]) -> Result<AppendResult, WalError> {
        let encoded = encode_record(self.number, self.write_position, payload)?;
        let encoded_size = u64::try_from(encoded.len()).map_err(|_| WalError::PositionExhausted)?;
        let next_position = self
            .write_position
            .checked_add(encoded_size)
            .ok_or(WalError::PositionExhausted)?;
        if next_position > self.max_size {
            if self.write_position == 0 {
                return Err(WalError::RecordTooLarge {
                    size: encoded_size,
                    max: self.max_size,
                });
            }
            return Ok(AppendResult::Full);
        }

        self.file.append(&encoded)?;
        self.write_position = next_position;
        Ok(AppendResult::Appended)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appends_and_reads_records_without_metadata_envelopes() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment = FileSegment::create(dir.path(), 1, 4096).unwrap();
        assert_eq!(segment.append(b"first").unwrap(), AppendResult::Appended);
        assert_eq!(segment.append(b"second").unwrap(), AppendResult::Appended);
        segment.file().sync(segment.write_position()).unwrap();

        let (next_position, records) = segment.read(0, 5).unwrap();
        assert_eq!(records, vec![Bytes::from_static(b"first")]);
        assert_eq!(next_position, 16);

        let (next_position, records) = segment.read(next_position, 6).unwrap();
        assert_eq!(records, vec![Bytes::from_static(b"second")]);
        assert_eq!(next_position, 33);
    }

    #[test]
    fn maximum_size_has_no_external_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment = FileSegment::create(dir.path(), 1, 16).unwrap();
        assert!(matches!(
            segment.append(b"first").unwrap(),
            AppendResult::Appended
        ));
        assert_eq!(segment.append(b"second").unwrap(), AppendResult::Full);
        assert_eq!(segment.file().size().unwrap(), 16);
    }

    #[test]
    fn read_rejects_a_first_record_larger_than_the_byte_limit() {
        let dir = tempfile::tempdir().unwrap();
        let mut segment = FileSegment::create(dir.path(), 1, 4096).unwrap();
        segment.append(b"record").unwrap();

        assert!(matches!(
            segment.read(0, 5),
            Err(WalError::ReadBufferTooSmall { size: 6, max: 5 })
        ));
    }
}
