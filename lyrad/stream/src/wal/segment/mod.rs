//! Buffered WAL segment files and RocksDB-style record framing.

use std::ffi::OsStr;
use std::fs::File;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};

mod codec;
#[allow(clippy::module_inception)]
mod segment;

pub(super) use super::WalError;
pub(super) use segment::{FileHandle, Segment};

const SEGMENT_EXTENSION: &str = "seg";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AppendResult {
    Appended(u64),
    Full,
}

pub(in crate::wal) fn make_segment_path(dir: &Path, segment_number: u64) -> PathBuf {
    dir.join(format!("{segment_number:010}.{SEGMENT_EXTENSION}"))
}

pub(in crate::wal) fn list_segment_files(dir: &Path) -> Result<Vec<(u64, PathBuf)>, WalError> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
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

pub(in crate::wal) fn sync_directory(dir: &Path) -> IoResult<()> {
    File::open(dir)?.sync_all()
}

fn invalid_segment_filename(path: &Path) -> WalError {
    WalError::corruption(
        path,
        "segment filename must contain exactly ten decimal digits",
    )
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
            WalError::Corruption {
                path: error_path,
                ..
            } if error_path == path
        ));
    }
}
