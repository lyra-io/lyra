+++
title = "Architecture"
description = "A guide to the responsibilities and current state of Lyra's four repositories."
weight = 2

[extra]
next_title = "Development guide"
next_path = "docs/development/"
+++

Lyra's source is organized into four independent repositories: `lyra-catalog`, `lyra-stream`, `lyra-meta`, and `lyra-func`. The website lives in a separate repository.

## Repository map

| Repository | Responsibility | Current state |
| --- | --- | --- |
| [lyra-catalog](https://github.com/lyra-io/lyra-catalog) | SQL catalog and PostgreSQL wire interface | Catalog statements, sessions, and authentication are implemented. |
| [lyra-stream](https://github.com/lyra-io/lyra-stream) | Stream storage foundations | WAL, segments, and virtual file systems are implemented. The unit service is not yet wired up. |
| [lyra-meta](https://github.com/lyra-io/lyra-meta) | Shared contracts, authentication, and metadata access | A shared Rust library with in-memory and Oxia metadata implementations. |
| [lyra-func](https://github.com/lyra-io/lyra-func) | Query task runtime | A scaffold; runtime execution remains planned work. |

These are repository boundaries. They do not imply that every component is currently a deployable service.

## The catalog

Catalog exposes a PostgreSQL wire interface and uses DataFusion for SQL planning. Its catalog statements manage databases, schemas, users, and secrets. Sessions select a database, and shared metadata supplies catalog identities and authentication data.

The CLI's `serve` command connects to Oxia using the `[meta]` configuration, constructs the catalog, and starts the listener configured by `[cata]`.

## The storage foundation

Stream contains a segmented write-ahead log, record encoding and CRC checks, and a virtual file system abstraction. Implementations cover buffered file I/O, direct I/O, and in-memory storage.

The repository also includes protocol definitions, client code, and a TLA+ specification. The unit runtime currently returns an unsupported error on startup; a runnable distributed stream service is still work to be completed.

## Shared metadata

Meta is a library consumed by the other repositories. It supplies catalog protobuf types, metadata interfaces, authentication helpers, and common utilities.

The metadata interface has both in-memory and Oxia-backed implementations. The in-memory implementation is useful for local tests. The catalog server uses the Oxia implementation for its metadata backend.

## Query execution

Func is reserved for query task execution. Its current source describes the intended role of a fragment as a source, transformation, exchange, or sink. The repository is a scaffold, and there is no implemented distributed execution service to start yet.

## Working across repositories

Catalog, Stream, and Func declare Meta as a Git dependency. Each repository has its own Cargo manifest and can be checked out independently.

Use the [development guide](@/docs/development.md) to work with a single repository or test a local change to the shared library across repositories.
