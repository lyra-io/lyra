//! RocksDB-style WAL record framing.
//!
//! ```text
//! Segment file
//! ┌─────────────────────────────────────────────────────────────┐
//! │ 32 KiB record block                                        │
//! │ ┌──────────┬───────┬──────┬───────────┬──────────────────┐ │
//! │ │ CRC32C:4 │ len:2 │ type │ segment:4 │ fragment payload │ │
//! │ └──────────┴───────┴──────┴───────────┴──────────────────┘ │
//! │ ... FULL or FIRST / MIDDLE / LAST fragments ...             │
//! │ zero trailer when fewer than 11 bytes remain in the block   │
//! ├─────────────────────────────────────────────────────────────┤
//! │ next 32 KiB record block                                    │
//! └─────────────────────────────────────────────────────────────┘
//! ```

mod crc;
mod record;
mod segment;

pub(super) use segment::{decode_record, encode_record};
