use super::StreamSegment;
use std::fs::{File, OpenOptions};
use std::io::Error;
use std::path::PathBuf;

const PAGE_SIZE: usize = 4096;

pub struct AlignedBuffer {
    ptr: *mut u8,
    len: usize,
    capacity: usize,
}

unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    pub fn new(capacity: usize) -> Self {
        let capacity = align_up(capacity, PAGE_SIZE);
        let ptr = unsafe {
            let mut p: *mut libc::c_void = std::ptr::null_mut();
            let ret = libc::posix_memalign(&mut p, PAGE_SIZE, capacity);
            assert_eq!(ret, 0, "posix_memalign failed");
            p as *mut u8
        };
        Self {
            ptr,
            len: 0,
            capacity,
        }
    }

    pub fn extend_from_slice(&mut self, data: &[u8]) {
        let new_len = self.len + data.len();
        if new_len > self.capacity {
            self.grow(new_len);
        }
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.add(self.len), data.len());
        }
        self.len = new_len;
    }

    pub fn pad_to_alignment(&mut self) {
        let aligned = align_up(self.len, PAGE_SIZE);
        if aligned > self.capacity {
            self.grow(aligned);
        }
        if aligned > self.len {
            unsafe {
                std::ptr::write_bytes(self.ptr.add(self.len), 0, aligned - self.len);
            }
            self.len = aligned;
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn grow(&mut self, min_capacity: usize) {
        let new_capacity = align_up(min_capacity.max(self.capacity * 2), PAGE_SIZE);
        let new_ptr = unsafe {
            let mut p: *mut libc::c_void = std::ptr::null_mut();
            let ret = libc::posix_memalign(&mut p, PAGE_SIZE, new_capacity);
            assert_eq!(ret, 0, "posix_memalign failed");
            std::ptr::copy_nonoverlapping(self.ptr, p as *mut u8, self.len);
            libc::free(self.ptr as *mut libc::c_void);
            p as *mut u8
        };
        self.ptr = new_ptr;
        self.capacity = new_capacity;
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.ptr as *mut libc::c_void);
        }
    }
}

fn align_up(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

pub struct DirectSegment {
    path: PathBuf,
    file: File,
    write_offset: u64,
    buf: AlignedBuffer,
}

impl DirectSegment {
    pub async fn new(path: PathBuf) -> Result<Self, Error> {
        Self::open(path, true, 0).await
    }

    pub async fn open_existing(path: PathBuf, write_offset: u64) -> Result<Self, Error> {
        Self::open(path, false, write_offset).await
    }

    async fn open(path: PathBuf, truncate: bool, write_offset: u64) -> Result<Self, Error> {
        let file = tokio::task::spawn_blocking({
            let path = path.clone();
            move || {
                let mut opts = OpenOptions::new();
                opts.create(true).read(true).write(true).truncate(truncate);

                #[cfg(target_os = "linux")]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    opts.custom_flags(libc::O_DIRECT);
                }

                let file = opts.open(&path)?;

                #[cfg(target_os = "macos")]
                {
                    use std::os::unix::io::AsRawFd;
                    unsafe {
                        libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1);
                    }
                }

                Ok::<File, Error>(file)
            }
        })
        .await
        .unwrap()?;

        Ok(Self {
            path,
            file,
            write_offset,
            buf: AlignedBuffer::new(PAGE_SIZE * 4),
        })
    }
}

#[async_trait::async_trait]
impl StreamSegment for DirectSegment {
    async fn write(&mut self, data: &[u8]) -> Result<u64, Error> {
        let offset_before = self.write_offset;
        let data_len = data.len() as u64;

        self.buf.clear();
        self.buf.extend_from_slice(data);
        self.buf.pad_to_alignment();

        let buf_slice = self.buf.as_slice().to_vec();
        let file = self.file.try_clone()?;
        let write_at = self.write_offset;

        tokio::task::spawn_blocking(move || {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&buf_slice, write_at)
        })
        .await
        .unwrap()?;

        self.write_offset += data_len;

        Ok(offset_before)
    }

    async fn sync(&self) -> Result<(), Error> {
        let file = self.file.try_clone()?;
        tokio::task::spawn_blocking(move || file.sync_data())
            .await
            .unwrap()
    }

    async fn read_all(&mut self) -> Result<Vec<u8>, Error> {
        let path = self.path.clone();
        let len = self.write_offset as usize;
        tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut file = File::open(&path)?;
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)?;
            Ok(buf)
        })
        .await
        .unwrap()
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        use std::os::unix::fs::FileExt;
        self.file.read_exact_at(buf, offset)
    }

    fn offset(&self) -> u64 {
        self.write_offset
    }

    fn size(&self) -> u64 {
        self.write_offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aligned_buffer_basic() {
        let mut buf = AlignedBuffer::new(PAGE_SIZE);
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.as_slice().as_ptr() as usize % PAGE_SIZE, 0);

        buf.extend_from_slice(b"hello world");
        assert_eq!(buf.len(), 11);
        assert_eq!(buf.as_slice(), b"hello world");
    }

    #[test]
    fn test_aligned_buffer_padding() {
        let mut buf = AlignedBuffer::new(PAGE_SIZE);
        buf.extend_from_slice(b"data");
        buf.pad_to_alignment();
        assert_eq!(buf.len() % PAGE_SIZE, 0);
        assert_eq!(buf.len(), PAGE_SIZE);
        assert_eq!(&buf.as_slice()[..4], b"data");
    }

    #[test]
    fn test_aligned_buffer_grow() {
        let mut buf = AlignedBuffer::new(PAGE_SIZE);
        let data = vec![0xABu8; PAGE_SIZE + 100];
        buf.extend_from_slice(&data);
        assert_eq!(buf.len(), PAGE_SIZE + 100);
        assert_eq!(buf.as_slice(), &data[..]);
    }

    #[test]
    fn test_aligned_buffer_clear() {
        let mut buf = AlignedBuffer::new(PAGE_SIZE);
        buf.extend_from_slice(b"some data");
        buf.clear();
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_aligned_buffer_round_trip() {
        let mut buf = AlignedBuffer::new(PAGE_SIZE);
        let original = b"round trip test data for alignment verification";
        buf.extend_from_slice(original);
        buf.pad_to_alignment();
        assert_eq!(&buf.as_slice()[..original.len()], &original[..]);
    }

    #[tokio::test]
    async fn test_direct_segment_write_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_direct.log");

        let mut seg = DirectSegment::new(path).await.unwrap();

        let data1 = b"first write";
        let offset1 = seg.write(data1).await.unwrap();
        assert_eq!(offset1, 0);

        let data2 = b"second write";
        let offset2 = seg.write(data2).await.unwrap();
        assert_eq!(offset2, data1.len() as u64);

        seg.sync().await.unwrap();

        let all = seg.read_all().await.unwrap();
        assert_eq!(&all[..data1.len()], data1);
        assert_eq!(&all[data1.len()..], data2);
    }
}
