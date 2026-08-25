//! Virtual filesystem interfaces reserved for stream storage.

mod direct;
mod memory;
mod standard;

pub use direct::{DirectFile, DirectVfs};
pub use memory::{MemoryFile, MemoryVfs};
pub use standard::{StandardFile, StandardVfs};

use bytes::Bytes;
use std::fmt::Debug;
use std::io::Result;
use std::path::{Path, PathBuf};

/// An opened file supplied by one of the supported virtual filesystems.
#[derive(Debug)]
pub enum VfsFile {
    Memory(MemoryFile),
    Standard(StandardFile),
    Direct(DirectFile),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenOptions {
    Existing,
    CreateNew,
}

/// Positioned reads and single-writer appends required by segments.
///
/// Calls to [`IoFile::append`] and [`IoFile::truncate`] must be serialized by
/// the caller. Reads and synchronization may run concurrently with the writer.
pub trait IoFile: Debug + Send + Sync {
    fn path(&self) -> &Path;

    fn size(&self) -> Result<u64>;

    /// Reads exactly `length` bytes starting at `position`.
    fn read_at(&self, position: u64, length: usize) -> Result<Bytes>;

    /// Appends all bytes and returns their starting file position.
    fn append(&self, bytes: &[u8]) -> Result<u64>;

    /// Removes bytes after `size` when rebuilding a segment tail.
    fn truncate(&self, size: u64) -> Result<()>;

    fn sync(&self) -> Result<()>;
}

/// Filesystem namespace and file-opening operations needed by segments.
pub trait Vfs: Debug + Send + Sync {
    /// Returns the required file-offset and I/O-length alignment, if any.
    fn alignment(&self) -> Option<u64>;

    fn create_dir(&self, path: &Path) -> Result<()>;

    fn open(&self, path: &Path, options: OpenOptions) -> Result<VfsFile>;

    fn list(&self, dir: &Path) -> Result<Vec<PathBuf>>;

    fn remove(&self, path: &Path) -> Result<()>;

    fn sync(&self, dir: &Path) -> Result<()>;
}

impl IoFile for VfsFile {
    fn path(&self) -> &Path {
        match self {
            Self::Memory(file) => file.path(),
            Self::Standard(file) => file.path(),
            Self::Direct(file) => file.path(),
        }
    }

    fn size(&self) -> Result<u64> {
        match self {
            Self::Memory(file) => file.size(),
            Self::Standard(file) => file.size(),
            Self::Direct(file) => file.size(),
        }
    }

    fn read_at(&self, position: u64, length: usize) -> Result<Bytes> {
        match self {
            Self::Memory(file) => file.read_at(position, length),
            Self::Standard(file) => file.read_at(position, length),
            Self::Direct(file) => file.read_at(position, length),
        }
    }

    fn append(&self, bytes: &[u8]) -> Result<u64> {
        match self {
            Self::Memory(file) => file.append(bytes),
            Self::Standard(file) => file.append(bytes),
            Self::Direct(file) => file.append(bytes),
        }
    }

    fn truncate(&self, size: u64) -> Result<()> {
        match self {
            Self::Memory(file) => file.truncate(size),
            Self::Standard(file) => file.truncate(size),
            Self::Direct(file) => file.truncate(size),
        }
    }

    fn sync(&self) -> Result<()> {
        match self {
            Self::Memory(file) => file.sync(),
            Self::Standard(file) => file.sync(),
            Self::Direct(file) => file.sync(),
        }
    }
}
