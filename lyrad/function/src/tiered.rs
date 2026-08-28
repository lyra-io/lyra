//! Built-in tiering function.
//!
//! This function consumes ordered Lyra records and streams prepared batches to
//! the table service. The table service, rather than the function, owns the
//! resulting L1/L2/L3 lifecycle.
