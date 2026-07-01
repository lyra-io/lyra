#[allow(clippy::module_inception)]
pub mod wal;

pub use crate::segment::Segment;

pub const INVALID_OFFSET: i64 = -1;
