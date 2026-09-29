+++
title = "Catalog SQL"
description = "Statements for working with databases, schemas, users, and secrets."
weight = 1

[extra]
next_title = "Catalog configuration"
next_path = "reference/configuration/"
+++

The catalog accepts SQL over its PostgreSQL wire listener. The examples below cover its catalog statements; they are not a claim of complete PostgreSQL SQL compatibility.

## Databases

Create a database and inspect the database list:

```sql
CREATE DATABASE analytics;
CREATE DATABASE IF NOT EXISTS analytics;
SHOW DATABASES;
SHOW DATABASES LIKE 'analytics%';
```

A new database receives a `public` schema. The default database is `dev`.

## Schemas

Create a schema within the current database, or qualify it with another database name:

```sql
CREATE SCHEMA events;
CREATE SCHEMA analytics.events;
SHOW SCHEMAS;
SHOW SCHEMAS LIKE 'event%';
```

`SHOW SCHEMAS` lists schemas in the current session's database. The current database is chosen when the client connects.

## Users

Create a user with a password:

```sql
CREATE USER reader WITH PASSWORD 'replace-this-password';
SHOW USERS;
```

To change a user's password:

```sql
ALTER USER reader WITH PASSWORD 'replace-with-a-new-password';
```

To rename or remove a user:

```sql
ALTER USER reader RENAME TO analyst;
DROP USER IF EXISTS analyst;
```

Catalog authentication uses SCRAM password data. Connect with an authenticated catalog session to run these statements. The bootstrap account configured in the server file provides the initial login.

## Secrets

Secrets are named values within a database and schema:

```sql
CREATE SECRET login_password VALUE 'replace-this-secret';
SHOW SECRETS;
```

Use a secret to provide a user's password:

```sql
CREATE USER service_reader PASSWORD SECRET login_password;
ALTER USER service_reader PASSWORD SECRET login_password;
```

The password is resolved when the user statement runs. Updating or removing the source secret does not automatically rotate the user's stored password. Run `ALTER USER ... PASSWORD SECRET ...` again to apply a new secret value.

## Session defaults

| Setting | Default |
| --- | --- |
| Database | `dev` |
| Schema | `public` |
| Sample bootstrap user | `root` |
| Sample listener | `127.0.0.1:5432` |

Choose the database explicitly in your connection string. See [getting started](@/docs/getting-started.md) for a complete psql example.

## Source of truth

Statement parsing lives in the catalog's [SQL parser](https://github.com/lyra-io/lyra-catalog/blob/main/catalog/src/sql/parser.rs). The [PostgreSQL integration test](https://github.com/lyra-io/lyra-catalog/blob/main/catalog/tests/pgwire_users.rs) exercises user login and secret-backed passwords.
