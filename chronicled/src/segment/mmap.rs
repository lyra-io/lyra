use super::{DEFAULT_MAX_SEGMENT_SIZE, Segment};
use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::io::Error;
use std::path::PathBuf;

pub struct MmapSegment {
    file: File,
    mmap: MmapMut,
    write_offset: u64,
    capacity: u64,
}

impl MmapSegment {
    pub async fn new(path: PathBuf) -> Result<Self, Error> {
        Self::with_capacity(path, DEFAULT_MAX_SEGMENT_SIZE).await
    }

    pub async fn with_capacity(path: PathBuf, initial_size: u64) -> Result<Self, Error> {
        Self::open(path, initial_size, true, 0).await
    }

    pub async fn open_existing(path: PathBuf, write_offset: u64) -> Result<Self, Error> {
        Self::open(path, DEFAULT_MAX_SEGMENT_SIZE, false, write_offset).await
    }

    async fn open(
        path: PathBuf,
        initial_size: u64,
        truncate: bool,
        write_offset: u64,
    ) -> Result<Self, Error> {
        let (file, mmap, capacity) = tokio::task::spawn_blocking({
            let path = path.clone();
            move || -> Result<(File, MmapMut, u64), Error> {
                let existing_len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                let capacity = initial_size.max(write_offset).max(existing_len);
                let file = OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(truncate)
                    .open(&path)?;

                file.set_len(capacity)?;

                let mmap = unsafe { MmapMut::map_mut(&file)? };
                Ok((file, mmap, capacity))
            }
        })
        .await
        .unwrap()?;

        Ok(Self {
            file,
            mmap,
            write_offset,
            capacity,
        })
    }

    fn grow(&mut self, required: u64) -> Result<(), Error> {
        let mut new_capacity = self.capacity * 2;
        while new_capacity < required {
            new_capacity *= 2;
        }

        self.file.set_len(new_capacity)?;
        self.mmap = unsafe { MmapMut::map_mut(&self.file)? };
        self.capacity = new_capacity;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Segment for MmapSegment {
    async fn write(&mut self, data: &[u8]) -> Result<u64, Error> {
        let offset_before = self.write_offset;
        let end = self.write_offset + data.len() as u64;

        if end > self.capacity {
            self.grow(end)?;
        }

        self.mmap[self.write_offset as usize..end as usize].copy_from_slice(data);
        self.write_offset = end;

        Ok(offset_before)
    }

    async fn sync(&self) -> Result<(), Error> {
        self.mmap.flush()
    }

    async fn read_all(&mut self) -> Result<Vec<u8>, Error> {
        Ok(self.mmap[..self.write_offset as usize].to_vec())
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        let start = offset as usize;
        let end = start + buf.len();
        if end > self.write_offset as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read past end of mmap segment",
            ));
        }
        buf.copy_from_slice(&self.mmap[start..end]);
        Ok(())
    }

    fn offset(&self) -> u64 {
        self.write_offset
    }

    fn size(&self) -> u64 {
        self.write_offset
    }

    fn as_std_file(&self) -> Option<&std::fs::File> {
        Some(&self.file)
    }

    fn advance_offset(&mut self, bytes: u64) {
        self.write_offset += bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mmap_segment_write_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_mmap.log");

        let mut seg = MmapSegment::with_capacity(path, 4096).await.unwrap();

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

    #[tokio::test]
    async fn test_mmap_segment_grow() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_mmap_grow.log");

        let mut seg = MmapSegment::with_capacity(path, 64).await.unwrap();

        let data = vec![0xABu8; 128];
        let offset = seg.write(&data).await.unwrap();
        assert_eq!(offset, 0);
        assert_eq!(seg.size(), 128);

        let all = seg.read_all().await.unwrap();
        assert_eq!(all, data);
    }

    #[tokio::test]
    async fn test_mmap_segment_offset_tracking() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_mmap_offset.log");

        let mut seg = MmapSegment::with_capacity(path, 4096).await.unwrap();
        assert_eq!(seg.offset(), 0);
        assert_eq!(seg.size(), 0);

        seg.write(b"hello").await.unwrap();
        assert_eq!(seg.offset(), 5);
        assert_eq!(seg.size(), 5);

        seg.write(b"world").await.unwrap();
        assert_eq!(seg.offset(), 10);
        assert_eq!(seg.size(), 10);
    }
}
