+++
title = "Getting started"
description = "Run Lyra Catalog locally and connect using the PostgreSQL wire protocol."
weight = 1

[extra]
next_title = "Catalog SQL reference"
next_path = "reference/catalog-sql/"
+++

This guide starts the catalog with an Oxia metadata backend, then connects with `psql` to create a database and schema.

## Before you begin

You will need:

- A Rust toolchain that supports edition 2024, with Cargo.
- The Protocol Buffers compiler, `protoc`, available on your `PATH`.
- Git and the PostgreSQL command-line client, `psql`.
- A running [Oxia service](https://github.com/oxia-db/oxia) with a `default` namespace, reachable from your machine. The sample Lyra configuration uses `127.0.0.1:6648`.

The catalog guide assumes Oxia is already running. It does not start an Oxia server for you.

## Get the source

Clone the catalog repository and enter its root directory:

```sh
git clone https://github.com/lyra-io/lyra-catalog.git
cd lyra-catalog
```

Cargo resolves the shared `lyra-meta` library from its Git repository. A separate local checkout of Meta is not required for this guide.

## Configure the catalog

Open `options/lyra-catalog.toml`. The checked-in development configuration is:

```toml
[meta]
service_address = "127.0.0.1:6648"
namespace = "default"

[cata]
host = "127.0.0.1"
port = 5432
max_connections = 0
bootstrap_user = "root"
bootstrap_password = "lyra"
```

Update the metadata address and namespace if your Oxia setup uses different values. The included `root` / `lyra` credentials are for local development; choose a different bootstrap password for a shared environment.

See the [configuration reference](@/reference/configuration.md) for the meaning of each field.

## Start the server

From the catalog repository root, run:

```sh
cargo run -p lyra-catalog-cli -- \
  serve --config options/lyra-catalog.toml
```

Keep this terminal open. The catalog initializes the default `dev` database and `public` schema in metadata and starts its PostgreSQL listener.

## Connect with psql

In a second terminal, connect to the default database:

```sh
psql "host=127.0.0.1 port=5432 user=root dbname=dev sslmode=disable" -W
```

Enter the `bootstrap_password` from your configuration when prompted. With the unmodified sample configuration, that password is `lyra`.

Specifying `dbname=dev` avoids psql's usual default of connecting to a database with the same name as the user. This example uses the local development listener without TLS.

## Try the catalog

Run these statements in psql:

```sql
CREATE DATABASE analytics;
CREATE SCHEMA analytics.events;
SHOW DATABASES;
```

The database list should include `analytics` and the default `dev` database. The schema statement creates `events` inside `analytics`.

To explore users and secrets, continue to the [SQL reference](@/reference/catalog-sql.md). This guide exercises the catalog; the Stream service and Func runtime are still under development.

## Troubleshooting

**The catalog cannot connect to metadata.** Check that Oxia is running, the address is reachable, and the configured namespace exists.

**Authentication fails.** Check the user and password in your configuration. Bootstrap creates the first user only when the catalog has no users; changing the bootstrap password in the file does not reset an existing user's password. Use `ALTER USER` from an authenticated session to change it.

**The database does not exist.** Connect to `dev` for the first session, or to a database you have already created.

**The build cannot find protoc.** Install the Protocol Buffers compiler and confirm that `protoc --version` works in the terminal running Cargo.
