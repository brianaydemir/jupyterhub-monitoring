# Monitoring and Reporting on JupyterHub Usage

A collection of command-line scripts for monitoring a
[JupyterHub](https://jupyterhub.readthedocs.io/) instance and reporting on its
usage. Scripts can push server snapshots to
[Elasticsearch](https://www.elastic.co/elasticsearch), query and report on the
data, and deliver reports by email.

## Prerequisites

- Python ≥ 3.14
- [Poetry](https://python-poetry.org/) (for installing from source)
- Docker (optional, for running via container)

## Installation

### From Source

```
git clone https://github.com/brianaydemir/jupyterhub-monitoring.git
cd jupyterhub-monitoring
make init       # runs: poetry install
```

Scripts are then available through Poetry:

```
poetry run push-servers --help
```

### Docker Image

Build the image with:

```
make build IMAGE=myregistry.example.com/me/jupyterhub-monitoring
```

Omit `IMAGE` to default to `jupyterhub-monitoring:<version>`. Run a script
inside the container by passing it as the command:

```
docker run --rm myregistry.example.com/me/jupyterhub-monitoring:latest \
    push-servers --help
```

## Configuration

### JupyterHub API Token

Scripts that talk to JupyterHub require an API token with the following scopes:

```
list:users  read:users  read:servers
```

Pass the token to a script via a file:

```
--api-key /path/to/jh-api-key.txt
```

or via the environment variable `JUPYTERHUB_API_KEY`.

### Elasticsearch API Key

Scripts that talk to Elasticsearch require an API key. Pass it via a file:

```
--api-key /path/to/es-api-key.txt
```

or via the environment variable `ELASTICSEARCH_API_KEY`.

Use `create-es-api-key`, `list-es-api-keys`, and `delete-es-api-key` to manage
Elasticsearch API keys (see below). These scripts authenticate with a username
and password rather than an API key.

## Scripts

All scripts accept `--help` for a full list of options.

### `push-servers`

Fetches the list of active servers from JupyterHub and indexes them as
documents in Elasticsearch. Use `--metadata KEY=VALUE` (repeatable) to attach
custom fields to every document (e.g., to identify the hub instance).

```
push-servers \
    --jupyterhub-endpoint https://hub.example.com/hub/api \
    --jupyterhub-api-key ~/secrets/jh-api-key.txt \
    --elasticsearch-endpoint https://elastic.example.com:9200 \
    --elasticsearch-api-key ~/secrets/es-api-key.txt \
    --elasticsearch-index jupyterhub-servers \
    --metadata hub=production
```

Use `--debug` to print documents to stdout instead of pushing them to
Elasticsearch.

### `get-new-users`

Lists JupyterHub users whose accounts were created within a given time window.
Optionally writes plain-text and/or HTML output files for use with
`send-email`.

```
get-new-users \
    --endpoint https://hub.example.com/hub/api \
    --api-key ~/secrets/jh-api-key.txt \
    --duration "7 days" \
    --text-file new-users.txt \
    --html-file new-users.html
```

Use `--time HH:MM` to anchor the end of the window to a specific wall-clock
time (within the past 24 hours) rather than the current moment.

### `get-new-activity`

Reports active server time per user within a given time window, pulling data
from Elasticsearch. Optionally writes plain-text and/or HTML output files.

```
get-new-activity \
    --endpoint https://elastic.example.com:9200 \
    --api-key ~/secrets/es-api-key.txt \
    --index jupyterhub-servers \
    --duration "24 hours" \
    --hub production \
    --text-file activity.txt \
    --html-file activity.html
```

### `get-es-docs`

Queries an Elasticsearch index using a Kibana-style query string and prints
matching documents as JSON.

```
get-es-docs \
    --endpoint https://elastic.example.com:9200 \
    --api-key ~/secrets/es-api-key.txt \
    --index jupyterhub-servers \
    --query "meta.hub:production AND user.admin:true"
```

### `create-es-api-key`

Creates an Elasticsearch API key using username and password authentication.
Prints the new key in the requested format.

```
create-es-api-key \
    --endpoint https://elastic.example.com:9200 \
    --username elastic \
    --name push-servers-key \
    --expiration 30d \
    --format key
```

### `list-es-api-keys`

Lists the active Elasticsearch API keys owned by the authenticated user.

```
list-es-api-keys \
    --endpoint https://elastic.example.com:9200 \
    --username elastic
```

Use `--all` to include expired and invalidated keys.

### `delete-es-api-key`

Invalidates an Elasticsearch API key by ID or by name.

```
# By ID:
delete-es-api-key \
    --endpoint https://elastic.example.com:9200 \
    --username elastic \
    --id AbCdEfGhIjKlMnOp

# By name:
delete-es-api-key \
    --endpoint https://elastic.example.com:9200 \
    --username elastic \
    --name push-servers-key
```

### `send-email`

Sends an email via SMTP. Pair with `get-new-users` or `get-new-activity` to
deliver reports.

```
send-email \
    --sender-email monitoring@example.com \
    --sender-name "JupyterHub Monitoring" \
    --recipient-email admin@example.com \
    --subject "Weekly New Users Report" \
    --smtp-host smtp.example.com \
    --smtp-port 465 \
    --text-file new-users.txt \
    --html-file new-users.html
```

Use `--no-ssl` if your SMTP server does not use SSL/TLS.
