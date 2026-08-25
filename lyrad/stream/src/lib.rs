//! Stateful storage for ordered Lyra streams.
//!
//! This crate owns mutable stream durability: write ordering, background WAL
//! workers, local segment rotation, synchronization, and lifecycle.

pub mod vfs;
pub mod wal;

pub use wal::{
    Log, LogError, LogOptions, PublishBatch, PublishRecord, PublishTarget, SegmentLog,
    SegmentOffset, Sequence,
};
