//! Alignment-aware direct-I/O filesystem implementation.

use super::standard::{ensure_range, invalid_input, read_once_at, write_all_at};
use super::{IoFile, OpenOptions, Vfs, VfsFile};
use bytes::Bytes;
use std::alloc::{Layout, alloc, alloc_zeroed, dealloc, handle_alloc_error};
use std::fs::File;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::fs::OpenOptions as FsOpenOptions;
use std::io::{Error, ErrorKind, Result};
#[cfg(target_os = "linux")]
use std::mem::MaybeUninit;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(target_os = "macos")]
const MACOS_BUFFER_ALIGNMENT: usize = 4096;

#[derive(Debug)]
pub struct DirectVfs {
    // Immutable state
    root: PathBuf,
    alignment: DirectAlignment,
}

#[derive(Debug)]
pub struct DirectFile {
    // Immutable state
    path: PathBuf,
    file: File,
    alignment: DirectAlignment,

    // Mutable state
    append_position: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
struct DirectAlignment {
    memory: usize,
    io: Option<usize>,
}

impl DirectVfs {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        let alignment = direct_io_alignment(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            alignment,
        })
    }
}

impl Vfs for DirectVfs {
    fn alignment(&self) -> Option<u64> {
        self.alignment.io.map(|alignment| alignment as u64)
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        std::fs::create_dir_all(path)
    }

    fn open(&self, path: &Path, options: OpenOptions) -> Result<VfsFile> {
        if path.parent() != Some(self.root.as_path()) {
            return Err(invalid_input(
                "direct-I/O file must be directly inside the VFS root",
            ));
        }
        open_file(path, options).and_then(|file| {
            let append_position = AtomicU64::new(file.metadata()?.len());
            Ok(VfsFile::Direct(DirectFile {
                path: path.to_path_buf(),
                file,
                alignment: self.alignment,
                append_position,
            }))
        })
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

impl IoFile for DirectFile {
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

        let io_alignment = self.alignment.io.unwrap_or(1);
        let offset_alignment = io_alignment as u64;
        let aligned_position = position / offset_alignment * offset_alignment;
        let prefix = usize::try_from(position - aligned_position)
            .map_err(|_| invalid_input("direct-I/O read position does not fit usize"))?;
        let needed = prefix
            .checked_add(length)
            .ok_or_else(|| invalid_input("direct-I/O read length overflows usize"))?;
        let aligned_len = needed
            .checked_next_multiple_of(io_alignment)
            .ok_or_else(|| invalid_input("aligned direct-I/O read length overflows usize"))?;
        let mut buffer = AlignedBuffer::zeroed(aligned_len, self.alignment.memory);
        let read = read_once_at(&self.file, buffer.as_mut_slice(), aligned_position)?;
        if read < needed {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "direct-I/O read returned fewer bytes than requested",
            ));
        }
        Ok(Bytes::copy_from_slice(&buffer.as_slice()[prefix..needed]))
    }

    fn append(&self, bytes: &[u8]) -> Result<u64> {
        let position = self.append_position.load(Ordering::Relaxed);
        let length = u64::try_from(bytes.len())
            .map_err(|_| invalid_input("file append length does not fit u64"))?;
        let next_position = position
            .checked_add(length)
            .ok_or_else(|| invalid_input("file append position overflows u64"))?;
        let io_alignment = self.alignment.io.unwrap_or(1);
        if !position.is_multiple_of(io_alignment as u64)
            || !bytes.len().is_multiple_of(io_alignment)
        {
            return Err(invalid_input(
                "direct-I/O write position and length must be aligned",
            ));
        }
        if bytes.is_empty() {
            return Ok(position);
        }

        if (bytes.as_ptr() as usize).is_multiple_of(self.alignment.memory) {
            write_all_at(&self.file, bytes, position)?;
        } else {
            let buffer = AlignedBuffer::from_slice(bytes, self.alignment.memory);
            write_all_at(&self.file, buffer.as_slice(), position)?;
        }
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

struct AlignedBuffer {
    // Immutable state
    ptr: NonNull<u8>,
    len: usize,
    layout: Layout,
}

impl AlignedBuffer {
    fn zeroed(len: usize, alignment: usize) -> Self {
        assert_ne!(len, 0);
        let layout = Layout::from_size_align(len, alignment).unwrap();
        let raw = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).unwrap_or_else(|| handle_alloc_error(layout));
        Self { ptr, len, layout }
    }

    fn from_slice(bytes: &[u8], alignment: usize) -> Self {
        assert!(!bytes.is_empty());
        let layout = Layout::from_size_align(bytes.len(), alignment).unwrap();
        let raw = unsafe { alloc(layout) };
        let ptr = NonNull::new(raw).unwrap_or_else(|| handle_alloc_error(layout));
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.as_ptr(), bytes.len());
        }
        Self {
            ptr,
            len: bytes.len(),
            layout,
        }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr.as_ptr(), self.layout) }
    }
}

fn open_file(path: &Path, open_options: OpenOptions) -> Result<File> {
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (path, open_options);
        return Err(Error::new(
            ErrorKind::Unsupported,
            "direct I/O is not supported on this platform",
        ));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let mut file_options = FsOpenOptions::new();
        file_options.read(true).write(true);
        if open_options == OpenOptions::CreateNew {
            file_options.create_new(true);
        }

        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            file_options.custom_flags(libc::O_DIRECT);
        }

        let file = file_options.open(path)?;

        #[cfg(target_os = "macos")]
        {
            use std::os::fd::AsRawFd;
            let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
            if result == -1 {
                return Err(Error::last_os_error());
            }
        }

        Ok(file)
    }
}

fn direct_io_alignment(root: &Path) -> Result<DirectAlignment> {
    if !root.is_dir() {
        return Err(invalid_input("direct-I/O VFS root is not a directory"));
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = root;
        return Err(Error::new(
            ErrorKind::Unsupported,
            "direct I/O is not supported on this platform",
        ));
    }

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let mut options = FsOpenOptions::new();
        options
            .read(true)
            .write(true)
            .custom_flags(libc::O_TMPFILE)
            .mode(0o600);
        let file = options.open(root)?;
        linux_direct_io_alignment(&file)
    }

    #[cfg(target_os = "macos")]
    {
        Ok(DirectAlignment {
            memory: MACOS_BUFFER_ALIGNMENT,
            io: None,
        })
    }
}

#[cfg(target_os = "linux")]
fn linux_direct_io_alignment(file: &File) -> Result<DirectAlignment> {
    let mut metadata = MaybeUninit::<libc::statx>::zeroed();
    let result = unsafe {
        libc::syscall(
            libc::SYS_statx,
            file.as_raw_fd(),
            b"\0".as_ptr().cast::<libc::c_char>(),
            libc::AT_EMPTY_PATH,
            libc::STATX_DIOALIGN,
            metadata.as_mut_ptr(),
        )
    };
    if result == -1 {
        let error = Error::last_os_error();
        return if matches!(
            error.raw_os_error(),
            Some(libc::ENOSYS) | Some(libc::EINVAL)
        ) {
            Err(Error::new(
                ErrorKind::Unsupported,
                "kernel does not report direct-I/O alignment",
            ))
        } else {
            Err(error)
        };
    }

    let metadata = unsafe { metadata.assume_init() };
    if metadata.stx_mask & libc::STATX_DIOALIGN == 0
        || metadata.stx_dio_mem_align == 0
        || metadata.stx_dio_offset_align == 0
    {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "filesystem does not report direct-I/O alignment",
        ));
    }

    let memory = metadata.stx_dio_mem_align as usize;
    if !memory.is_power_of_two() {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "direct-I/O memory alignment is not a power of two",
        ));
    }
    Ok(DirectAlignment {
        memory,
        io: Some(metadata.stx_dio_offset_align as usize),
    })
}

pub(crate) fn direct_io_unavailable(error: &Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::InvalidInput | ErrorKind::Unsupported
    ) || matches!(
        error.raw_os_error(),
        Some(libc::EINVAL) | Some(libc::ENOSYS) | Some(libc::ENOTSUP)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligned_buffer_has_aligned_address_and_length() {
        let alignment = 4096;
        let bytes = vec![0xAB; alignment * 2];
        let buffer = AlignedBuffer::from_slice(&bytes, alignment);
        assert_eq!(buffer.as_slice(), bytes);
        assert!((buffer.as_slice().as_ptr() as usize).is_multiple_of(alignment));
        assert!(buffer.as_slice().len().is_multiple_of(alignment));
    }
}
