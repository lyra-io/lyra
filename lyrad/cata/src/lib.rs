//! Query and cluster control plane for Lyra.
//!
//! Cata parses and plans queries, maintains the cluster view through Oxia,
//! and distributes execution tasks to Func runtimes.

pub mod options;

mod cata;
mod error;
mod handler;
mod session;
mod sql;

pub use cata::Cata;
pub use error::{CataError, Result};
