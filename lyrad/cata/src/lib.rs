//! Query and cluster control plane for Lyra.
//!
//! Cata parses and plans queries, maintains the cluster view through Oxia,
//! and distributes execution tasks to Func runtimes.

pub mod options;

mod authentication;
mod cata;
mod error;
mod executor;
mod handler;
mod sql;

pub use cata::Cata;
pub use error::{CataError, Result};
