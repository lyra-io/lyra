//! Stream-processing functions for Lyra.
//!
//! This crate is the boundary for functions that consume ordered Lyra records,
//! produce Arrow batches, and send those batches to the table service over a
//! streaming RPC.

pub mod tiered;
