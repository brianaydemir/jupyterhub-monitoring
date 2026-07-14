# Monitoring and Reporting on JupyterHub Usage

A CLI toolkit for monitoring a
[JupyterHub](https://jupyterhub.readthedocs.io/) instance
and reporting on its usage.

The general workflow is:

1. Run `push-servers` on a schedule
   to snapshot the live server list into Elasticsearch.

2. Run `get-new-activity` or `get-new-users` to generate reports.

The remaining commands
(`create-es-api-key`, `delete-es-api-key`, `list-es-api-keys`, `get-es-docs`)
are operational helpers for managing Elasticsearch access.

## External Services

- **JupyterHub** (API endpoint + API admin key) —
  required for `push-servers` and `get-new-users`

- **Elasticsearch** (API endpoint + API key or credentials) —
  required for `push-servers`, `get-new-activity`, and all `*-es-*` commands

- **SMTP relay** —
  required only when using `--send-email`

## Commands

### `push-servers`

Fetches the current server list from JupyterHub
and pushes each server as a timestamped document to an Elasticsearch index.
Intended to run on a schedule
(e.g., every few minutes via cron or a Kubernetes CronJob)
to build a history of server activity.

Each document uses flattened dotted-key paths
(e.g., `server.ready`, `meta.hub`)
combining the JupyterHub server object
with metadata and a timestamp.
The required `--interval` flag records the push cadence
(e.g., `--interval 5m`)
so that `get-new-activity` can compute per-user uptime.
The `--metadata` flag tags documents
with additional identifying context
(e.g., `--metadata hub=prod testing=false`);
`get-new-activity` can filter on `meta.hub` via `--hub`.

### `get-new-activity`

Queries Elasticsearch for server-snapshot documents in a time window
and aggregates per-user server uptime.
A document contributes to a user's total when
`server.ready == true` or `server.pending == "spawn"`;
each such document adds its `meta.interval` value
to that user's running total.
Documents missing `meta.interval`
(e.g., those pushed before `--interval` was introduced)
are silently skipped.
Documents where `meta.testing` is `"true"` or `true`
are automatically excluded.
Users with multiple active servers
accumulate time from each server independently.

### `get-new-users`

Fetches the user list from JupyterHub
and reports on accounts created within a specified time window.

### `create-es-api-key`, `delete-es-api-key`, `list-es-api-keys`

Create, invalidate, and list Elasticsearch API keys
owned by the authenticated user.

### `get-es-docs`

Queries an Elasticsearch index with a Kibana-style query string
and prints raw JSON documents.
Useful for ad-hoc inspection or debugging of the index.

## Output Formats

`get-new-activity` and `get-new-users`
can write output to any combination of
plain text, styled HTML, CSV, and Excel files,
and/or deliver the report by email.
Each report table is automatically attached as a CSV file when sending email.
Run either command with `--help` for the full list of flags.

## Username Parsing

JupyterHub usernames encode the authentication source.
The report commands parse them to populate
**Institution**, **ID**, and **Login method** columns.

| Username format             | Login method |
| :-------------------------- | :----------- |
| `user@example.edu`          | local NetID  |
| `eppn:user@example.edu`     | legacy eppn  |
| `email:user@example.edu`    | email        |
| `orcid:0009-0008-3064-0494` | ORCID        |
