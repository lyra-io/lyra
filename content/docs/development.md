+++
title = "Development"
description = "Find the right repository, build from source, and make a focused contribution."
weight = 3

[extra]
next_title = "Architecture overview"
next_path = "docs/architecture/"
+++

## Choose a repository

Work in the repository that owns the behavior you want to change:

| Area | Repository |
| --- | --- |
| SQL, catalog statements, or PostgreSQL sessions | [lyra-catalog](https://github.com/lyra-io/lyra-catalog) |
| WAL, segments, VFS, or stream protocol | [lyra-stream](https://github.com/lyra-io/lyra-stream) |
| Metadata interfaces, shared types, or authentication | [lyra-meta](https://github.com/lyra-io/lyra-meta) |
| Query task runtime design | [lyra-func](https://github.com/lyra-io/lyra-func) |
| Documentation or the website | [lyra-io.github.io](https://github.com/lyra-io/lyra-io.github.io) |

Each Rust repository contains an `AGENTS.md` with conventions for module layout, imports, and stateful structs. Read it before editing.

## Build and test

Install a Rust toolchain supporting edition 2024 and the Protocol Buffers compiler (`protoc`). Then, from the root of the repository you are working on:

```sh
cargo check --workspace
cargo test --workspace
cargo fmt --all -- --check
```

Some tests depend on external services. In Meta, the Oxia metadata integration test is ignored by default. To run it against a development Oxia instance:

```sh
OXIA_SERVICE_ADDRESS=127.0.0.1:6648 \
  cargo test --test oxia_metadata -- --ignored
```

This test writes temporary test records to the `default` namespace. Use a development instance.

## Test a local Meta change

Consumers normally resolve `lyra-meta` from its Git repository. To try an unpublished Meta change, check out Meta next to the consumer repository and add this temporary override to the consumer's root `Cargo.toml`:

```toml
[patch."https://github.com/lyra-io/lyra-meta"]
lyra-meta = { path = "../lyra-meta" }
```

Run the consumer's checks and tests with the override. Remove the local override before submitting a change intended to build from the shared Git dependency.

## Work on the website

The website uses Zola. Its deployment workflow currently builds with Zola `0.23.6`.

```sh
git clone https://github.com/lyra-io/lyra-io.github.io.git
cd lyra-io.github.io
zola serve
```

Open the local URL printed by Zola. Content is written in Markdown under `content/`; shared layouts live in `templates/`, styles in `sass/`, and browser assets in `static/`.

Before submitting website changes:

```sh
zola check --skip-external-links
zola build
```

## Propose a change

Open an issue or pull request in the relevant repository. Describe the problem, the resulting behavior, and the checks you ran. For an architectural change that spans repositories, include the affected contracts and the order in which consumers would need to adopt it.

Lyra's code is licensed under Apache 2.0. The license file in each repository contains the full terms.
