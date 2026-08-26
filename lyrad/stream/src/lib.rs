//! Stateful storage for ordered Lyra streams.
//!
//! This crate owns mutable stream durability: write ordering, background WAL
//! workers, local segment rotation, synchronization, and lifecycle.

pub mod vfs;
pub mod wal;

pub use wal::{Log, LogOptions, PublishBatch, PublishTarget, SegmentLog, Sequence, WalError};
