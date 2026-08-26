//! Streaming adapter for positioned segment VFS reads.

use crate::vfs::{IoFile, VfsFile};
use std::io::{Read, Result};

pub(super) struct SegmentReader<'a> {
    pub(super) file: &'a VfsFile,
    pub(super) position: u64,
    pub(super) end: u64,
}

impl Read for SegmentReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> Result<usize> {
        if output.is_empty() || self.position >= self.end {
            return Ok(0);
        }
        let output_len = u64::try_from(output.len()).unwrap_or(u64::MAX);
        let length = usize::try_from((self.end - self.position).min(output_len)).unwrap();
        let bytes = self.file.read_at(self.position, length)?;
        output[..length].copy_from_slice(&bytes);
        self.position += length as u64;
        Ok(length)
    }
}
