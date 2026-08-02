mod error;
mod format;
mod io;

pub use error::SegmentError;
pub use io::SegmentFile;

pub(crate) use format::{FILE_HEADER_SIZE, SegmentRecord, SegmentScanner, encode_batch};
pub(crate) use io::{AlignedBuffer, list_segment_files, sync_directory};

#[cfg(test)]
pub(crate) use format::encode_file_header;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoMode {
    DirectPreferred,
    DirectRequired,
    Standard,
}
