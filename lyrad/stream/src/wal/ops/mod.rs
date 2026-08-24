mod append;
mod sync;

pub(super) use append::AppendOp;
pub(super) use sync::SyncOp;

pub(super) enum Operation {
    Append(AppendOp),
    Sync(SyncOp),
    Close,
}
