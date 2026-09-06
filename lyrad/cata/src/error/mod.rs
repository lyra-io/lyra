mod cata;
mod pgwire;

pub use cata::{CataError, Result};
pub(crate) use pgwire::to_pgwire_error;
