//! Stateless tiered storage for Lyra.
//!
//! This crate is the boundary for future request-scoped operations over
//! immutable local and remote data. The existing stateful storage logic lives
//! in `stream` and is intentionally not shared with this crate.
