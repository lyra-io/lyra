use super::IoMode;
use super::error::SegmentError;
use super::format::{ALIGNMENT, encode_file_header};
use bytes::Bytes;
use std::alloc::{Layout, alloc, alloc_zeroed, dealloc, handle_alloc_error};
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::Arc;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

const SEGMENT_EXTENSION: &str = "seg";

pub(crate) struct AlignedBuffer {
    ptr: NonNull<u8>,
    len: usize,
    layout: Layout,
}

unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    fn zeroed(len: usize) -> Self {
        assert_ne!(len, 0);
        assert_eq!(len % ALIGNMENT, 0);
        let layout = Layout::from_size_align(len, ALIGNMENT).unwrap();
        let raw = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).unwrap_or_else(|| handle_alloc_error(layout));
        Self { ptr, len, layout }
    }

    pub(crate) fn from_slice(bytes: &[u8]) -> Self {
        assert!(!bytes.is_empty());
        assert_eq!(bytes.len() % ALIGNMENT, 0);
        let layout = Layout::from_size_align(bytes.len(), ALIGNMENT).unwrap();
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

    pub(crate) fn as_slice(&self) -> &[u8] {
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

pub struct SegmentFile {
    path: PathBuf,
    file: File,
    direct: bool,
}

impl SegmentFile {
    pub fn open(path: impl AsRef<Path>, io_mode: IoMode) -> Result<Self, SegmentError> {
        let path = path.as_ref().to_path_buf();
        match io_mode {
            IoMode::Standard => Self::open_with_mode(path, false).map_err(Into::into),
            IoMode::DirectRequired => Self::open_with_mode(path, true).map_err(Into::into),
            IoMode::DirectPreferred => match Self::open_with_mode(path.clone(), true) {
                Ok(file) => Ok(file),
                Err(error) if direct_io_unavailable(&error) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %error,
                        "direct I/O unavailable for segment read; falling back to standard I/O"
                    );
                    Self::open_with_mode(path, false).map_err(Into::into)
                }
                Err(error) => Err(error.into()),
            },
        }
    }

    pub(crate) fn create(
        dir: &Path,
        segment_number: u64,
        io_mode: IoMode,
    ) -> Result<Arc<Self>, SegmentError> {
        let path = segment_path(dir, segment_number);
        let header = AlignedBuffer::from_slice(&encode_file_header(segment_number));

        match io_mode {
            IoMode::Standard => Self::create_with_mode(path, false, &header)
                .map(Arc::new)
                .map_err(Into::into),
            IoMode::DirectRequired => Self::create_with_mode(path, true, &header)
                .map(Arc::new)
                .map_err(Into::into),
            IoMode::DirectPreferred => match Self::create_with_mode(path.clone(), true, &header) {
                Ok(file) => Ok(Arc::new(file)),
                Err(error) if direct_io_unavailable(&error) => {
                    cleanup_failed_create(&path)?;
                    tracing::warn!(
                        path = %path.display(),
                        error = %error,
                        "direct I/O unavailable; falling back to standard I/O"
                    );
                    Self::create_with_mode(path, false, &header)
                        .map(Arc::new)
                        .map_err(Into::into)
                }
                Err(error) => Err(error.into()),
            },
        }
    }

    fn create_with_mode(
        path: PathBuf,
        direct: bool,
        header: &AlignedBuffer,
    ) -> std::io::Result<Self> {
        let file = open_new_file(&path, direct)?;
        let segment = Self { path, file, direct };
        segment.write_aligned(header, 0)?;
        Ok(segment)
    }

    fn open_with_mode(path: PathBuf, direct: bool) -> std::io::Result<Self> {
        let file = open_existing_file(&path, direct)?;
        Ok(Self { path, file, direct })
    }

    pub fn read_at(&self, position: u64, length: usize) -> Result<Bytes, SegmentError> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        let end = position
            .checked_add(length as u64)
            .ok_or_else(|| invalid_input("segment read range overflows u64"))?;
        if end > self.file.metadata()?.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "segment read extends past end of file",
            )
            .into());
        }

        if !self.direct {
            let mut bytes = vec![0; length];
            read_exact_at(&self.file, &mut bytes, position)?;
            return Ok(Bytes::from(bytes));
        }

        let aligned_position = position / ALIGNMENT as u64 * ALIGNMENT as u64;
        let prefix = usize::try_from(position - aligned_position)
            .map_err(|_| invalid_input("segment read position does not fit usize"))?;
        let needed = prefix
            .checked_add(length)
            .ok_or_else(|| invalid_input("segment read length overflows usize"))?;
        let aligned_len = needed
            .checked_next_multiple_of(ALIGNMENT)
            .ok_or_else(|| invalid_input("aligned segment read length overflows usize"))?;
        let mut buffer = AlignedBuffer::zeroed(aligned_len);
        let read = read_once_at(&self.file, buffer.as_mut_slice(), aligned_position)?;
        if read < needed {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "direct segment read returned fewer bytes than requested",
            )
            .into());
        }
        Ok(Bytes::copy_from_slice(&buffer.as_slice()[prefix..needed]))
    }

    pub fn len(&self) -> Result<u64, SegmentError> {
        Ok(self.file.metadata()?.len())
    }

    pub fn is_empty(&self) -> Result<bool, SegmentError> {
        Ok(self.len()? == 0)
    }

    pub(crate) fn write_aligned(&self, buffer: &AlignedBuffer, offset: u64) -> std::io::Result<()> {
        let bytes = buffer.as_slice();
        if self.direct
            && (!(offset as usize).is_multiple_of(ALIGNMENT)
                || !bytes.len().is_multiple_of(ALIGNMENT)
                || !(bytes.as_ptr() as usize).is_multiple_of(ALIGNMENT))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "direct I/O write is not aligned",
            ));
        }

        #[cfg(unix)]
        {
            self.file.write_all_at(bytes, offset)
        }

        #[cfg(not(unix))]
        {
            let _ = (bytes, offset);
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "segment files currently require Unix positioned I/O",
            ))
        }
    }

    pub(crate) fn sync_data(&self) -> std::io::Result<()> {
        self.file.sync_data()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub(crate) fn segment_path(dir: &Path, segment_number: u64) -> PathBuf {
    dir.join(format!("{segment_number:010}.{SEGMENT_EXTENSION}"))
}

pub(crate) fn list_segment_files(dir: &Path) -> Result<Vec<(u64, PathBuf)>, SegmentError> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension() != Some(OsStr::new(SEGMENT_EXTENSION)) {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(OsStr::to_str) else {
            continue;
        };
        let Ok(segment_number) = stem.parse::<u64>() else {
            continue;
        };
        files.push((segment_number, path));
    }
    files.sort_by_key(|(segment_number, _)| *segment_number);
    Ok(files)
}

pub(crate) fn sync_directory(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

fn open_new_file(path: &Path, direct: bool) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.create_new(true).read(true).write(true);

    #[cfg(target_os = "linux")]
    if direct {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_DIRECT);
    }

    let file = options.open(path)?;

    #[cfg(target_os = "macos")]
    if direct {
        use std::os::fd::AsRawFd;
        let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
        if result == -1 {
            return Err(std::io::Error::last_os_error());
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    if direct {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "direct I/O is not supported on this platform",
        ));
    }

    Ok(file)
}

fn open_existing_file(path: &Path, direct: bool) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);

    #[cfg(target_os = "linux")]
    if direct {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_DIRECT);
    }

    let file = options.open(path)?;

    #[cfg(target_os = "macos")]
    if direct {
        use std::os::fd::AsRawFd;
        let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
        if result == -1 {
            return Err(std::io::Error::last_os_error());
        }
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    if direct {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "direct I/O is not supported on this platform",
        ));
    }

    Ok(file)
}

fn invalid_input(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

#[cfg(unix)]
fn read_once_at(file: &File, bytes: &mut [u8], position: u64) -> std::io::Result<usize> {
    loop {
        match file.read_at(bytes, position) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

#[cfg(not(unix))]
fn read_once_at(_file: &File, _bytes: &mut [u8], _position: u64) -> std::io::Result<usize> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "segment files currently require Unix positioned I/O",
    ))
}

fn read_exact_at(file: &File, mut bytes: &mut [u8], mut position: u64) -> std::io::Result<()> {
    while !bytes.is_empty() {
        let read = read_once_at(file, bytes, position)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "segment read extends past end of file",
            ));
        }
        position += read as u64;
        bytes = &mut bytes[read..];
    }
    Ok(())
}

fn cleanup_failed_create(path: &Path) -> Result<(), SegmentError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn direct_io_unavailable(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::InvalidInput | std::io::ErrorKind::Unsupported
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
        let bytes = vec![0xAB; ALIGNMENT * 2];
        let buffer = AlignedBuffer::from_slice(&bytes);
        assert_eq!(buffer.as_slice(), bytes);
        assert_eq!(buffer.as_slice().as_ptr() as usize % ALIGNMENT, 0);
        assert_eq!(buffer.as_slice().len() % ALIGNMENT, 0);
    }

    #[test]
    fn standard_segment_writes_header() {
        let dir = tempfile::tempdir().unwrap();
        let file = SegmentFile::create(dir.path(), 1, IoMode::Standard).unwrap();
        file.sync_data().unwrap();
        assert_eq!(
            std::fs::metadata(file.path()).unwrap().len(),
            ALIGNMENT as u64
        );
    }

    #[test]
    fn reads_exact_unaligned_ranges_in_standard_and_direct_modes() {
        for io_mode in [IoMode::Standard, IoMode::DirectPreferred] {
            let dir = tempfile::tempdir().unwrap();
            let file = SegmentFile::create(dir.path(), 1, io_mode).unwrap();
            let body = AlignedBuffer::from_slice(&vec![0xAB; ALIGNMENT]);
            file.write_aligned(&body, ALIGNMENT as u64).unwrap();
            assert_eq!(
                file.read_at(0, 8).unwrap(),
                Bytes::from_static(b"LYRASEG\0")
            );
            assert_eq!(file.read_at(1, 7).unwrap(), Bytes::from_static(b"YRASEG\0"));
            assert_eq!(
                file.read_at(ALIGNMENT as u64 - 3, 10).unwrap(),
                Bytes::from_static(&[0, 0, 0, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB])
            );
            assert!(file.read_at((ALIGNMENT * 2) as u64, 1).is_err());
        }
    }
}
