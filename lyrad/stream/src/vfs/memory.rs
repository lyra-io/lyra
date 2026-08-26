//! In-memory virtual filesystem implementation.

use super::{IoFile, OpenOptions, Vfs, VfsFile};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Default)]
pub struct MemoryVfs {
    // Mutable state
    state: Arc<RwLock<MemoryState>>,
}

#[derive(Debug)]
pub struct MemoryFile {
    // Immutable state
    path: PathBuf,

    // Mutable state
    data: Arc<RwLock<Vec<u8>>>,
}

#[derive(Debug, Default)]
struct MemoryState {
    // Mutable state
    directories: HashSet<PathBuf>,
    files: HashMap<PathBuf, Arc<RwLock<Vec<u8>>>>,
}

impl Vfs for MemoryVfs {
    fn alignment(&self) -> Option<u64> {
        None
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        let mut state = self.state.write().unwrap();
        for ancestor in path.ancestors() {
            state.directories.insert(ancestor.to_path_buf());
        }
        Ok(())
    }

    fn open(&self, path: &Path, options: OpenOptions) -> Result<VfsFile> {
        let data = match options {
            OpenOptions::Existing => self
                .state
                .read()
                .unwrap()
                .files
                .get(path)
                .cloned()
                .ok_or_else(|| Error::new(ErrorKind::NotFound, "file does not exist"))?,
            OpenOptions::CreateNew => {
                let mut state = self.state.write().unwrap();
                if state.files.contains_key(path) {
                    return Err(Error::new(ErrorKind::AlreadyExists, "file already exists"));
                }
                let data = Arc::new(RwLock::new(Vec::new()));
                state.files.insert(path.to_path_buf(), Arc::clone(&data));
                data
            }
        };
        Ok(VfsFile::Memory(MemoryFile {
            path: path.to_path_buf(),
            data,
        }))
    }

    fn list(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        let state = self.state.read().unwrap();
        if !state.directories.contains(dir) {
            return Err(Error::new(ErrorKind::NotFound, "directory does not exist"));
        }
        Ok(state
            .files
            .keys()
            .filter(|path| path.parent() == Some(dir))
            .cloned()
            .collect())
    }

    fn remove(&self, path: &Path) -> Result<()> {
        let mut state = self.state.write().unwrap();
        state
            .files
            .remove(path)
            .map(|_| ())
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "file does not exist"))
    }

    fn sync(&self, dir: &Path) -> Result<()> {
        let state = self.state.read().unwrap();
        if state.directories.contains(dir) {
            Ok(())
        } else {
            Err(Error::new(ErrorKind::NotFound, "directory does not exist"))
        }
    }
}

impl IoFile for MemoryFile {
    fn path(&self) -> &Path {
        &self.path
    }

    fn size(&self) -> Result<u64> {
        u64::try_from(self.data.read().unwrap().len())
            .map_err(|_| Error::new(ErrorKind::InvalidData, "file length exceeds u64"))
    }

    fn read_at(&self, position: u64, length: usize) -> Result<Bytes> {
        let position = usize::try_from(position)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "position does not fit usize"))?;
        let end = position
            .checked_add(length)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "read range overflows usize"))?;
        let data = self.data.read().unwrap();
        let bytes = data
            .get(position..end)
            .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "read extends past end of file"))?;
        Ok(Bytes::copy_from_slice(bytes))
    }

    fn append(&self, bytes: &[u8]) -> Result<u64> {
        let mut data = self.data.write().unwrap();
        let position = u64::try_from(data.len())
            .map_err(|_| Error::new(ErrorKind::InvalidData, "file length exceeds u64"))?;
        data.extend_from_slice(bytes);
        Ok(position)
    }

    fn truncate(&self, size: u64) -> Result<()> {
        let size = usize::try_from(size)
            .map_err(|_| Error::new(ErrorKind::InvalidInput, "file length does not fit usize"))?;
        self.data.write().unwrap().truncate(size);
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        Ok(())
    }

    fn discard_cache(&self, _end: u64) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn files_share_contents_between_handles() {
        let vfs = MemoryVfs::default();
        let dir = Path::new("/wal");
        let path = dir.join("0000000001.seg");
        vfs.create_dir(dir).unwrap();

        let created = vfs.open(&path, OpenOptions::CreateNew).unwrap();
        assert_eq!(created.append(b"record").unwrap(), 0);
        let opened = vfs.open(&path, OpenOptions::Existing).unwrap();

        assert_eq!(opened.read_at(0, 6).unwrap(), Bytes::from_static(b"record"));
        assert_eq!(vfs.list(dir).unwrap(), vec![path]);
    }
}
