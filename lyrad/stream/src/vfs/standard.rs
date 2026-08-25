//! Standard page-cache-backed filesystem implementation.

use super::{IoFile, OpenOptions, Vfs, VfsFile};
use bytes::Bytes;
use std::fs::{File, OpenOptions as FsOpenOptions};
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[derive(Debug, Default)]
pub struct StandardVfs;

#[derive(Debug)]
pub struct StandardFile {
    // Immutable state
    path: PathBuf,
    file: File,

    // Mutable state
    append_position: AtomicU64,
}

impl Vfs for StandardVfs {
    fn alignment(&self) -> Option<u64> {
        None
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        std::fs::create_dir_all(path)
    }

    fn open(&self, path: &Path, options: OpenOptions) -> Result<VfsFile> {
        let mut file_options = FsOpenOptions::new();
        file_options.read(true).write(true);
        if options == OpenOptions::CreateNew {
            file_options.create_new(true);
        }
        let file = file_options.open(path)?;
        let append_position = AtomicU64::new(file.metadata()?.len());
        Ok(VfsFile::Standard(StandardFile {
            path: path.to_path_buf(),
            file,
            append_position,
        }))
    }

    fn list(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        std::fs::read_dir(dir)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect()
    }

    fn remove(&self, path: &Path) -> Result<()> {
        std::fs::remove_file(path)
    }

    fn sync(&self, dir: &Path) -> Result<()> {
        File::open(dir)?.sync_all()
    }
}

impl IoFile for StandardFile {
    fn path(&self) -> &Path {
        &self.path
    }

    fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read_at(&self, position: u64, length: usize) -> Result<Bytes> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        ensure_range(self.file.metadata()?.len(), position, length)?;
        let mut bytes = vec![0; length];
        read_exact_at(&self.file, &mut bytes, position)?;
        Ok(Bytes::from(bytes))
    }

    fn append(&self, bytes: &[u8]) -> Result<u64> {
        let position = self.append_position.load(Ordering::Relaxed);
        let length = u64::try_from(bytes.len())
            .map_err(|_| invalid_input("file append length does not fit u64"))?;
        let next_position = position
            .checked_add(length)
            .ok_or_else(|| invalid_input("file append position overflows u64"))?;
        write_all_at(&self.file, bytes, position)?;
        self.append_position.store(next_position, Ordering::Relaxed);
        Ok(position)
    }

    fn truncate(&self, size: u64) -> Result<()> {
        self.file.set_len(size)?;
        self.append_position.store(size, Ordering::Relaxed);
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.file.sync_data()
    }
}

pub(super) fn ensure_range(file_len: u64, position: u64, length: usize) -> Result<()> {
    let end = position
        .checked_add(length as u64)
        .ok_or_else(|| invalid_input("file read range overflows u64"))?;
    if end > file_len {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "file read extends past end of file",
        ));
    }
    Ok(())
}

pub(super) fn invalid_input(message: &'static str) -> Error {
    Error::new(ErrorKind::InvalidInput, message)
}

#[cfg(unix)]
pub(super) fn read_once_at(file: &File, bytes: &mut [u8], position: u64) -> Result<usize> {
    loop {
        match file.read_at(bytes, position) {
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

#[cfg(not(unix))]
pub(super) fn read_once_at(_file: &File, _bytes: &mut [u8], _position: u64) -> Result<usize> {
    Err(Error::new(
        ErrorKind::Unsupported,
        "positioned file I/O is not supported on this platform",
    ))
}

pub(super) fn read_exact_at(file: &File, mut bytes: &mut [u8], mut position: u64) -> Result<()> {
    while !bytes.is_empty() {
        let read = read_once_at(file, bytes, position)?;
        if read == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "file read extends past end of file",
            ));
        }
        position += read as u64;
        bytes = &mut bytes[read..];
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn write_all_at(file: &File, bytes: &[u8], position: u64) -> Result<()> {
    file.write_all_at(bytes, position)
}

#[cfg(not(unix))]
pub(super) fn write_all_at(_file: &File, _bytes: &[u8], _position: u64) -> Result<()> {
    Err(Error::new(
        ErrorKind::Unsupported,
        "positioned file I/O is not supported on this platform",
    ))
}
