use super::StreamSegment;
use std::io::Error;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub struct StandardSegment {
    pub path: PathBuf,
    file: File,
    std_file: std::fs::File,
    write_offset: u64,
}

impl StandardSegment {
    pub async fn new(path: PathBuf) -> Result<Self, Error> {
        Self::open(path, true, 0).await
    }

    pub async fn open_existing(path: PathBuf, write_offset: u64) -> Result<Self, Error> {
        Self::open(path, false, write_offset).await
    }

    async fn open(path: PathBuf, truncate: bool, write_offset: u64) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(truncate)
            .open(&path)
            .await?;

        let std_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;

        let mut segment = StandardSegment {
            path,
            file,
            std_file,
            write_offset,
        };
        segment
            .file
            .seek(std::io::SeekFrom::Start(write_offset))
            .await?;
        Ok(segment)
    }

    pub async fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize, Error> {
        self.file.seek(std::io::SeekFrom::Start(offset)).await?;
        self.file.read(buf).await
    }
}

#[async_trait::async_trait]
impl StreamSegment for StandardSegment {
    async fn write(&mut self, data: &[u8]) -> Result<u64, Error> {
        let offset_before = self.write_offset;
        self.file.write_all(data).await?;
        self.write_offset += data.len() as u64;
        Ok(offset_before)
    }

    async fn sync(&self) -> Result<(), Error> {
        self.file.sync_data().await
    }

    async fn read_all(&mut self) -> Result<Vec<u8>, Error> {
        self.file.seek(std::io::SeekFrom::Start(0)).await?;
        let mut buf = Vec::new();
        self.file.read_to_end(&mut buf).await?;
        Ok(buf)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        self.std_file.read_exact_at(buf, offset)
    }

    fn offset(&self) -> u64 {
        self.write_offset
    }

    fn size(&self) -> u64 {
        self.write_offset
    }

    fn as_std_file(&self) -> Option<&std::fs::File> {
        Some(&self.std_file)
    }

    fn advance_offset(&mut self, bytes: u64) {
        self.write_offset += bytes;
    }
}
