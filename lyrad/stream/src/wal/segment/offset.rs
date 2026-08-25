//! Physical position of one logical record in a WAL segment.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentOffset {
    segment_number: u64,
    position: u64,
}

impl SegmentOffset {
    /// Creates a physical WAL record position.
    pub const fn new(segment_number: u64, position: u64) -> Self {
        Self {
            segment_number,
            position,
        }
    }

    /// Returns the containing segment number.
    pub const fn segment_number(self) -> u64 {
        self.segment_number
    }

    /// Returns the record's byte position within its segment.
    pub const fn position(self) -> u64 {
        self.position
    }
}
