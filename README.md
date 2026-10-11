# Monitoring and Reporting on JupyterHub Usage

A CLI toolkit
for monitoring a [JupyterHub](https://jupyterhub.readthedocs.io/) instance
and reporting on its usage.
It snapshots the live server list
into [Elasticsearch](https://www.elastic.co/elasticsearch),
reports on the accumulated history, and can deliver those reports by email.

## Commands

| Command                                                      | Purpose                                       |
| :----------------------------------------------------------- | :-------------------------------------------- |
| `push-servers`                                               | Snapshot the hub's servers into Elasticsearch |
| `get-new-activity`                                           | Report on server activity from the snapshots  |
| `get-new-users`                                              | Report on accounts created on the hub         |
| `create-es-api-key`, `list-es-api-keys`, `delete-es-api-key` | Manage Elasticsearch API keys                 |
| `get-es-docs`                                                | Print documents from an Elasticsearch index   |

Run `push-servers` on a schedule, such as from cron or a Kubernetes CronJob,
with `--interval` set to that schedule's period.
Every command documents its options under `--help`.

See [docs/snapshots.md](docs/snapshots.md)
for what `push-servers` records and how the reports count it,
and [docs/migrating-from-2.x.md](docs/migrating-from-2.x.md)
when upgrading from 2.x.

## Installation

Python 3.14 or later is required.

### From Source

```
git clone https://github.com/brianaydemir/jupyterhub-monitoring.git
cd jupyterhub-monitoring
make init
poetry run push-servers --help
```

### Docker Image

```
make build IMAGE=registry.example.com/me/jupyterhub-monitoring
```

The image is tagged with the version from `pyproject.toml`.
It runs as an unprivileged user,
so pass credentials through the environment:

```
docker run --rm \
    -e JUPYTERHUB_API_KEY \
    -e ELASTICSEARCH_API_KEY \
    registry.example.com/me/jupyterhub-monitoring:3.0.0 \
    push-servers \
        --jupyterhub-endpoint https://hub.example.com/hub/api \
        --es-endpoint https://es.example.com:9200 \
        --es-index jupyterhub-servers \
        --interval 5m
```

## Exit Codes

| Code | Meaning                                                         |
| :--- | :-------------------------------------------------------------- |
| 0    | Success                                                         |
| 1    | The command ran but failed                                      |
| 2    | Invalid command-line arguments, reported by the argument parser |

A command exits 1 when a service call fails,
when a credential is missing or unreadable,
when `push-servers` could not push at least one document,
or when `--send-email` could not deliver the report.

## License

Apache License 2.0.
See [LICENSE](LICENSE).
