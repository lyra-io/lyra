//! Compatibility facade for Lyra's storage crates.

/// Stateful storage for ordered streams.
pub use lyra_stream_storage as stream_storage;
/// Stateless operations over immutable tiered data.
pub use lyra_tiered_storage as tiered_storage;

// Preserve the original public paths while consumers migrate to the
// domain-specific crates.
pub use lyra_stream_storage::segment;
pub use lyra_stream_storage::wal;
