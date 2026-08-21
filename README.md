<img src="logo.svg" alt="Lyra" width="180" align="left">

<h3>Lyra</h3>

<p>The next generation of distributed streaming systems.</p>

[![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024%20edition-orange?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![Stars](https://img.shields.io/github/stars/lyra-io/lyra?style=flat-square&logo=github)](https://github.com/lyra-io/lyra)

<br clear="left">

## Overview

Lyra is a Rust workspace for building a distributed streaming system. The project currently focuses on protocol definitions, ordered local stream durability, client APIs, a command-line interface, and the boundary for tiered storage.

## Workspace

- `lyrad/meta` — generated gRPC services and wire-protocol types.
- `lyrad/stream` — stateful stream storage, including WAL ordering, recovery, reads, synchronization, and segment rotation.
- `lyrad/tiered` — stateless operations over immutable local and remote data.
- `lyra/liblyra` — client-facing events, connections, errors, and metrics.
- `lyra/cli` — the `lyra` command-line interface.

## Stream durability

The stream WAL serializes appends through a dedicated worker thread. Records are stored in checksummed 64 MiB segments, recovered in sequence order after restart, and readable by sequence number. Direct I/O is preferred where supported, with a standard-I/O fallback.

## Development

Use the Rust toolchain configured for the workspace, then run:

```sh
cargo test --workspace
cargo clippy -p stream --all-targets -- -D warnings
```
