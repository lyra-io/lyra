//! Physical segment format and local I/O used by stream storage.

mod error;
mod file_segment;
mod files;
mod format;
pub mod vfs;

pub use error::SegmentError;
pub use file_segment::FileSegment;
pub use vfs::{DirectVfs, IoFile, MemoryVfs, OpenOptions, StandardVfs, Vfs, VfsFile};

pub(crate) use files::{list_segment_files, sync};
#[cfg(test)]
pub(crate) use format::FILE_HEADER_SIZE;

use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    /// Prefer unbuffered direct I/O, falling back to standard I/O when the
    /// platform or filesystem does not support it.
    DirectPreferred,
    /// Require unbuffered direct I/O; opening fails when unsupported.
    DirectRequired,
    /// Use the operating system page cache for all I/O.
    Standard,
}

/// A logical record position within one segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentOffset(u64);

/// The result of attempting to append one record to a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendResult {
    Appended(SegmentOffset),
    Full,
}

/// Record-level operations supported by a local segment.
pub trait Segment {
    /// Appends one payload or reports that the record area is full.
    fn append(&mut self, payload: &[u8]) -> Result<AppendResult, SegmentError>;

    /// Reads one payload by its segment-local offset.
    fn read(&self, offset: SegmentOffset) -> Result<Option<Bytes>, SegmentError>;

    /// Makes the segment immutable and writes its index and footer.
    fn seal(&mut self) -> Result<(), SegmentError>;
}
