//! Physical segment format and local I/O used by stream storage.

mod codec;
mod error;
mod seg;
pub mod vfs;

pub use error::SegmentError;
pub use seg::FileSegment;
pub use vfs::{DirectVfs, IoFile, MemoryVfs, OpenOptions, StandardVfs, Vfs, VfsFile};

#[cfg(test)]
pub(crate) use codec::FILE_HEADER_SIZE;

use bytes::Bytes;
use std::ffi::OsStr;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};

const SEGMENT_EXTENSION: &str = "seg";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    /// Prefer unbuffered direct I/O, falling back to standard I/O when the
    /// platform or filesystem does not support it.
    DirectPreferred,
    /// Require unbuffered direct I/O; opening fails when unsupported.
    DirectRequired,
    /// Use the operating system page cache for all I/O.
    Standard,
}

/// A logical record position within one segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentOffset(u64);

/// The result of attempting to append one record to a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendResult {
    Appended(SegmentOffset),
    Full,
}

pub(crate) fn segment_path(dir: &Path, segment_number: u64) -> PathBuf {
    dir.join(format!("{segment_number:010}.{SEGMENT_EXTENSION}"))
}

pub(crate) fn list_segment_files(dir: &Path) -> Result<Vec<(u64, PathBuf)>, SegmentError> {
    let mut files = Vec::new();
    for path in StandardVfs.list(dir)? {
        if path.extension() != Some(OsStr::new(SEGMENT_EXTENSION)) {
            continue;
        }
        let stem = path
            .file_stem()
            .and_then(OsStr::to_str)
            .ok_or_else(|| invalid_segment_filename(&path))?;
        if stem.len() != 10 || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid_segment_filename(&path));
        }
        let segment_number = stem
            .parse::<u64>()
            .map_err(|_| invalid_segment_filename(&path))?;
        files.push((segment_number, path));
    }
    files.sort_by_key(|(segment_number, _)| *segment_number);
    Ok(files)
}

pub(crate) fn sync(dir: &Path) -> IoResult<()> {
    StandardVfs.sync(dir)
}

fn invalid_segment_filename(path: &Path) -> SegmentError {
    SegmentError::Corruption {
        path: path.to_path_buf(),
        message: "segment filename must contain exactly ten decimal digits".into(),
    }
}

/// Record-level operations supported by a local segment.
pub trait Segment {
    /// Appends one payload or reports that the record area is full.
    fn append(&mut self, payload: &[u8]) -> Result<AppendResult, SegmentError>;

    /// Reads one payload by its segment-local offset.
    fn read(&self, offset: SegmentOffset) -> Result<Option<Bytes>, SegmentError>;

    /// Makes the segment immutable and writes its index and footer.
    fn seal(&mut self) -> Result<(), SegmentError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_segment_filenames_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1.seg");
        std::fs::write(&path, []).unwrap();

        let error = list_segment_files(dir.path()).unwrap_err();
        assert!(matches!(
            error,
            SegmentError::Corruption {
                path: error_path,
                ..
            } if error_path == path
        ));
    }
}
