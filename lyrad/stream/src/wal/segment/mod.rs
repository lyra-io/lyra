//! Buffered WAL segment files and RocksDB-style record framing.

use bytes::Bytes;
use std::io::Read;

mod codec;
mod error;
mod file;
mod files;
mod offset;
#[allow(clippy::module_inception)]
mod segment;

pub(super) use error::SegmentError;
pub(super) use file::SegmentFile;
pub(super) use files::{list_segment_files, sync_directory};
pub use offset::SegmentOffset;
pub(super) use segment::FileSegment;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AppendResult {
    Appended(SegmentOffset),
    Full,
}

pub(super) trait Segment {
    fn read<R: Read>(
        &self,
        reader: &mut R,
        position: u64,
        file_end: u64,
    ) -> Result<(u64, Bytes), SegmentError>;

    fn append(&mut self, payload: &[u8]) -> Result<AppendResult, SegmentError>;
}
