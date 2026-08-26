//! Buffered WAL segment files and RocksDB-style record framing.

use crate::vfs::{Vfs, VfsI};
use bytes::Bytes;
use std::ffi::OsStr;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};

mod codec;
mod seg;
mod seg_reader;

pub(super) use super::WalError;
pub(super) use seg::FileSegment;

const SEGMENT_EXTENSION: &str = "seg";

/// One append-only WAL segment.
///
/// Positions are physical byte offsets in the encoded segment file. The WAL
/// owns rotation and recovery; a segment only appends, reads, synchronizes, or
/// truncates its own file. The caller must serialize append and truncate calls.
pub(super) trait Segment {
    /// Reads complete logical records starting at `position` without exceeding
    /// `max_bytes` of decoded payload, and returns the next unread position.
    fn read(&self, position: u64, max_bytes: usize) -> Result<(u64, Vec<Bytes>), WalError>;

    /// Makes all bytes visible before synchronization durable.
    fn sync(&self) -> Result<(), WalError>;

    /// Appends one logical record or returns [`WalError::SegmentFull`] without
    /// modifying the segment when rotation is required.
    fn append(&self, payload: &[u8]) -> Result<(), WalError>;

    /// Removes bytes at and after `position`.
    fn truncate(&self, position: u64) -> Result<(), WalError>;
}

pub(in crate::wal) fn make_segment_path(dir: &Path, segment_number: u64) -> PathBuf {
    dir.join(format!("{segment_number:010}.{SEGMENT_EXTENSION}"))
}

pub(in crate::wal) fn list_segments(
    vfs: &VfsI,
    dir: &Path,
) -> Result<Vec<(u64, PathBuf)>, WalError> {
    let mut files = Vec::new();
    for path in vfs.list(dir)? {
        if path.extension() != Some(OsStr::new(SEGMENT_EXTENSION)) {
            continue;
        }
        let stem = path.file_stem().and_then(OsStr::to_str).ok_or_else(|| {
            WalError::corruption(
                &path,
                "segment filename must contain exactly ten decimal digits",
            )
        })?;
        if stem.len() != 10 || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(WalError::corruption(
                &path,
                "segment filename must contain exactly ten decimal digits",
            ));
        }
        let segment_number = stem.parse::<u64>().map_err(|_| {
            WalError::corruption(
                &path,
                "segment filename must contain exactly ten decimal digits",
            )
        })?;
        files.push((segment_number, path));
    }
    files.sort_by_key(|(segment_number, _)| *segment_number);
    Ok(files)
}

/// Persists segment creation and other directory-entry changes.
pub(in crate::wal) fn sync_all(vfs: &VfsI, dir: &Path) -> IoResult<()> {
    vfs.sync(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_segment_filenames_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("1.seg");
        std::fs::write(&path, []).unwrap();

        let vfs = crate::vfs::VfsI::Standard(crate::vfs::StandardVfs);
        let error = list_segments(&vfs, dir.path()).unwrap_err();
        assert!(matches!(
            error,
            WalError::Corruption {
                path: error_path,
                ..
            } if error_path == path
        ));
    }
}
