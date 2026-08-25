//! Segment filename and directory operations.

use super::SegmentError;
use super::vfs::{StandardVfs, Vfs};
use std::ffi::OsStr;
use std::io::Result as IoResult;
use std::path::{Path, PathBuf};

const SEGMENT_EXTENSION: &str = "seg";

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
