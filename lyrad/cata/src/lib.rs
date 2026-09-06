//! Query and cluster control plane for Lyra.
//!
//! Cata parses and plans queries, maintains the cluster view through Oxia,
//! and distributes execution tasks to Func runtimes.

mod error;
mod options;
mod server;

pub use error::{CataError, Result};
pub use options::CataOptions;
pub use server::Cata;
