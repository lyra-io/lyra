+++
title = "Independent repositories. A shared foundation."
description = "A look at the repository split: where the code lives now, what each part owns, and how the pieces stay connected."
date = 2026-09-29

[extra]
category = "Project notes"
+++

Lyra's code now lives in four repositories: Catalog, Stream, Meta, and Func. The extraction commits landed on September 28, 2026, giving each part its own source tree and Cargo manifest.

This note is a map of that structure and the current implementation.

## Where the code lives

**[lyra-catalog](https://github.com/lyra-io/lyra-catalog)** owns the SQL catalog and PostgreSQL wire interface. Its source includes catalog statement parsing and execution, session handling, and user authentication.

**[lyra-stream](https://github.com/lyra-io/lyra-stream)** owns the storage foundation: the write-ahead log, segments, virtual file systems, protocol definitions, and client code. The repository also carries the TLA+ specification.

**[lyra-meta](https://github.com/lyra-io/lyra-meta)** contains the shared library. Catalog types, metadata interfaces, authentication helpers, and common utilities belong here.

**[lyra-func](https://github.com/lyra-io/lyra-func)** is the home for the query task runtime. At this stage it is a scaffold.

The website and documentation live in [lyra-io.github.io](https://github.com/lyra-io/lyra-io.github.io).

## A shared library across repositories

Catalog, Stream, and Func declare Meta as a Git dependency. Shared types and metadata interfaces therefore remain in one library, while each consumer has its own workspace.

For a change that affects more than one repository, a local Cargo patch can connect a Meta checkout to a consumer. The [development guide](@/docs/development.md) explains that workflow.

## What is available today

The catalog has a runnable server using Oxia for metadata and the PostgreSQL wire protocol for client connections. Its implemented catalog operations cover databases, schemas, users, and secrets.

Stream has WAL and file-system implementations, but its unit runtime still needs to be connected. Func does not yet provide an execution runtime. Splitting the repositories does not change those implementation boundaries.

The [architecture guide](@/docs/architecture.md) describes each area in more detail.

## Finding your next step

To try the catalog, follow [getting started](@/docs/getting-started.md). For statement syntax or server options, use the [reference](@/reference/_index.md).

To contribute, start in the repository that owns the behavior you want to change. A shared contract change belongs in Meta; its effects on Catalog, Stream, or Func should be considered alongside it.
