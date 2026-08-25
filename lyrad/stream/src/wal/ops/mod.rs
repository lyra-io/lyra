mod append;
mod sync;

pub(super) use append::AppendOp;
pub(super) use sync::{SyncFile, SyncOp};

pub(super) enum Operation {
    Append(AppendOp),
    Sync(SyncOp),
    Close,
}
