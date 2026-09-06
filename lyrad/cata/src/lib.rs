//! Query and cluster control plane for Lyra.
//!
//! Cata parses and plans queries, maintains the cluster view through Oxia,
//! and distributes execution tasks to Func runtimes.

pub mod options;

mod error;
mod planner;
mod postgres;
mod query;
mod service;

pub use error::{CataError, Result};
pub use service::Cata;
