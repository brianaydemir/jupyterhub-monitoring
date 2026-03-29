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

Each document has the shape
`{server: <JupyterHub server object>, meta: <metadata + timestamp + interval>}`.
The `--metadata` flag tags documents with identifying context
(e.g., `--metadata hub=prod testing=false`);
`get-new-activity` can later filter on those values.

### `get-new-activity`

Queries Elasticsearch for server-snapshot documents in a time window
and aggregates per-user server uptime.
A document contributes to a user's total when
`server.ready == true` or `server.pending == "spawn"`;
each document adds `meta.interval` seconds to that user's running total.

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
**Institution**, **ID**, and **Login Method** columns.

| Username format             | Login Method |
| :-------------------------- | :----------- |
| `user@example.edu`          | ePPN         |
| `eppn:user@example.edu`     | ePPN         |
| `email:user@example.edu`    | Email        |
| `orcid:0009-0008-3064-0494` | ORCID        |
