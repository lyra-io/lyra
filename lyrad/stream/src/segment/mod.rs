//! Physical segment format and local I/O used by stream storage.

mod error;
mod format;
mod io;

pub use error::SegmentError;
pub use io::SegmentFile;

pub(crate) use format::{FILE_HEADER_SIZE, SegmentRecord, encode_batch, scan_segment};
pub(crate) use io::{AlignedBuffer, list_segment_files, sync_directory};

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
