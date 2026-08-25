//! Record framing, encoding, decoding, and scanning.
//!
//! ```text
//! Segment file
//! ┌──────────────────────────────────────────────────────────────┐
//! │ 4 KiB file header: magic, version, segment, block, alignment │
//! ├──────────────────────────────────────────────────────────────┤
//! │ logical record 0                                             │
//! │   physical fragment(s) + zero padding to VFS alignment       │
//! ├──────────────────────────────────────────────────────────────┤
//! │ logical record 1                                             │
//! │   physical fragment(s) + zero padding to VFS alignment       │
//! ├──────────────────────────────────────────────────────────────┤
//! │ ... active record area ...                                   │
//! ├──────────────────────────────────────────────────────────────┤
//! │ sealed index: [record file position: u64]... + zero padding  │
//! ├──────────────────────────────────────────────────────────────┤
//! │ 4 KiB footer: segment, record size/count, index size + CRCs   │
//! └──────────────────────────────────────────────────────────────┘
//!
//! One logical record inside 32 KiB record blocks
//! ┌──────────┬────────┬──────┬─────────────┬─────────────────────┐
//! │ CRC32C:4 │ len:2  │ type │ segment:4   │ fragment payload    │
//! └──────────┴────────┴──────┴─────────────┴─────────────────────┘
//!                         type = FULL | FIRST | MIDDLE | LAST
//! ```

mod crc;
mod record;
mod segment;

#[cfg(test)]
pub(crate) use record::PHYSICAL_HEADER_SIZE;
pub(crate) use record::{encode_record, read_record, scan_file};
pub(crate) use segment::{
    ALIGNMENT, FILE_HEADER_SIZE, encode_file_header, encode_index_footer, load_index,
    read_file_header,
};
