# Project Instructions

## Rust imports

- Import referenced types into scope and use their short names instead of repeating fully qualified paths. For example, prefer `use segment::SegmentError;` and `SegmentError::Io` over `crate::segment::SegmentError::Io`.

## Rust module layout

- Keep `mod.rs` files declarative. They may contain module declarations, exports and re-exports, interfaces, shared type declarations, and constants.
- Do not put operational logic or function implementations in `mod.rs`; place them in clearly named submodules instead.
- In `mod.rs`, place traits after module declarations, exports and re-exports, type aliases, and constants.

## Rust implementation helpers

- Use numbered suffixes for private implementation layers, such as `open0` and `open1`, instead of names such as `open_inner`.

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
