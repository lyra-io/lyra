//! Buffered file handle shared by WAL segment readers, writers, and sync work.

use super::SegmentError;
use std::fs::{File, OpenOptions};
use std::io::{Result as IoResult, Write};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
pub(in crate::wal) struct SegmentFile {
    // Immutable state
    path: PathBuf,
    file: File,

    // Mutable state
    evicted_position: AtomicU64,
}

impl SegmentFile {
    pub(in crate::wal) fn create(path: &Path) -> Result<Self, SegmentError> {
        Self::open0(path, true)
    }

    pub(in crate::wal) fn open(path: &Path) -> Result<Self, SegmentError> {
        Self::open0(path, false)
    }

    fn open0(path: &Path, create_new: bool) -> Result<Self, SegmentError> {
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

    pub(in crate::wal) fn size(&self) -> Result<u64, SegmentError> {
        Ok(self.file.metadata()?.len())
    }

    pub(in crate::wal) fn reader(&self) -> Result<File, SegmentError> {
        Ok(self.file.try_clone()?)
    }

    pub(in crate::wal) fn append(&self, bytes: &[u8]) -> Result<(), SegmentError> {
        (&self.file).write_all(bytes)?;
        Ok(())
    }

    pub(in crate::wal) fn truncate(&self, size: u64) -> Result<(), SegmentError> {
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
        // Linux ignores partial pages for POSIX_FADV_DONTNEED, so retain the
        // active tail and only advance through complete pages.
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
