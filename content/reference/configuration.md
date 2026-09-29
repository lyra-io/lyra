+++
title = "Configuration"
description = "Configure the catalog's metadata connection, listener, and bootstrap account."
weight = 2

[extra]
next_title = "Getting started"
next_path = "docs/getting-started/"
+++

The catalog `serve` command takes a TOML configuration file:

```sh
cargo run -p lyra-catalog-cli -- \
  serve --config options/lyra-catalog.toml
```

The file has two sections: `[meta]` for Oxia and `[cata]` for the catalog listener.

## Metadata connection

```toml
[meta]
service_address = "127.0.0.1:6648"
namespace = "default"
```

| Field | Type | Meaning |
| --- | --- | --- |
| `service_address` | String | Address of the Oxia service used to store catalog metadata. |
| `namespace` | String | Oxia namespace used by the metadata client. |

Both fields are required. The namespace must be available in your Oxia deployment.

## Catalog listener

```toml
[cata]
host = "127.0.0.1"
port = 5432
max_connections = 0
bootstrap_user = "root"
bootstrap_password = "lyra"
```

| Field | Type | Sample value | Meaning |
| --- | --- | --- | --- |
| `host` | String | `"127.0.0.1"` | Address to bind the PostgreSQL listener. |
| `port` | Integer | `5432` | Port for PostgreSQL client connections. |
| `max_connections` | Integer | `0` | Connection limit passed to the PostgreSQL server. |
| `bootstrap_user` | String, optional | `"root"` | Initial user to create when the catalog has no users. |
| `bootstrap_password` | String, optional | `"lyra"` | Password for the new bootstrap user. |

`host`, `port`, and `max_connections` must be present in the file. Bootstrap credentials can be omitted together, or supplied together. Providing only one causes startup to fail.

## Bootstrap behavior

The bootstrap configuration creates the initial user only when the catalog has no users. It does not reset an existing account's password or add a bootstrap account to a catalog that already has users. An empty catalog requires both bootstrap fields to start successfully.

The checked-in password is a local development example. To change an existing account, use an authenticated SQL session and `ALTER USER` as described in the [SQL reference](@/reference/catalog-sql.md).

## Validation

The configuration parser rejects unknown fields. Startup also fails if the file cannot be read, the TOML is invalid, or only one of the bootstrap credential fields is supplied.

Keep your configuration aligned with the [sample file](https://github.com/lyra-io/lyra-catalog/blob/main/options/lyra-catalog.toml) and the [CLI configuration parser](https://github.com/lyra-io/lyra-catalog/blob/main/cli/src/serve.rs).
