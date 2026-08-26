# Project Instructions

## Rust imports

- Import referenced types into scope and use their short names instead of repeating fully qualified paths. For example, prefer `use wal::WalError;` and `WalError::Io` over `crate::wal::WalError::Io`.

## Rust module layout

- Keep `mod.rs` files declarative. They may contain module declarations, exports and re-exports, interfaces, shared type declarations, and constants.
- Do not put operational logic or function implementations in `mod.rs`; place them in clearly named submodules instead.
- As an explicit exception, `wal/segment/mod.rs` may contain small segment namespace utilities such as path construction and directory listing or syncing.
- In `mod.rs`, place traits after module declarations, exports and re-exports, type aliases, and constants.

## Rust implementation helpers

- Use numbered suffixes for private implementation layers, such as `open0` and `open1`, instead of names such as `open_inner`.
- Use associated `Type::new` functions for type constructors.
- Reserve the `make_` prefix for free utilities that derive standalone values such as paths, names, or static-like strings, for example `make_segment_path`.
- Keep short, single-use logic inline instead of extracting a helper that is only several straightforward lines.

## Stateful Rust structs

- Group fields in this order: control state, immutable state, then mutable state.
- Add `// Control state`, `// Immutable state`, and `// Mutable state` comments to make the groups explicit.
- Within control state, declare an execution or cancellation context first and background task handles immediately after it.
- Follow the same field order in struct initializers when practical.

Example:

```rust
pub struct Service {
    // Control state
    context: CancellationToken,
    tasks: Mutex<Option<JoinSet<()>>>,

    // Immutable state
    request_tx: mpsc::Sender<Request>,
    options: ServiceOptions,

    // Mutable state
    state: Arc<RwLock<State>>,
}
```
