# Migrating from 2.x

Release 3.0.0 renames the connection flags of every command,
folds the `send-email` command into the report commands,
and changes how a few figures are counted.
The tables below compare it with 2.4.0, the last 2.x release.

## Connection Flags

Every flag that names a service now says which one:
`--jupyterhub-endpoint`, `--jupyterhub-api-key`, and `--jupyterhub-ca-cert`
for JupyterHub,
and `--es-endpoint`, `--es-api-key`, `--es-ca-cert`, `--es-index`,
and `--es-username` for Elasticsearch.

| Command             | 2.4.0                      | 3.0.0                   |
| :------------------ | :------------------------- | :---------------------- |
| `push-servers`      | `--elasticsearch-endpoint` | `--es-endpoint`         |
| `get-new-users`     | `--endpoint`               | `--jupyterhub-endpoint` |
| every other command | `--endpoint`               | `--es-endpoint`         |

`--api-key`, `--ca-cert`, `--index`, and `--username`
are renamed the same way as `--endpoint`.

## Email

The `send-email` command is gone.
Pass `--send-email` to `get-new-activity` or `get-new-users` instead,
with the same SMTP flags,
and the run emails the report it just produced:
its text and HTML renderings as the body,
each of its tables as a CSV attachment,
and the workbook when `--xlsx-file` is also given.
Every email flag is an error without `--send-email`.

| `send-email` in 2.4.0                                 | `--send-email` in 3.0.0                  |
| :---------------------------------------------------- | :--------------------------------------- |
| `--text-file` and `--html-file` as the body           | (none; the report is the body)           |
| `--attachment`                                        | (none; attachments are automatic)        |
| `--no-subject-heading`                                | (none)                                   |
| `--no-ssl`                                            | `--smtp-no-ssl`                          |
| `--recipient-email` (one address)                     | one or more; list them or repeat it      |
| `--recipient-name`                                    | (none; write it in `--recipient-email`)  |
| `--recipient-email root`                              | rejected; an address needs an `@`        |
| `--subject` default of "JupyterHub Monitoring Report" | the report's name and the window's dates |

## Other Flags

| 2.4.0                                             | 3.0.0                                          |
| :------------------------------------------------ | :--------------------------------------------- |
| `--duration`, required                            | or `--report-start` with `--report-end`        |
| `--metadata interval=5m`                          | `--interval 5m`, now required                  |
| reserved `--metadata` keys dropped with a warning | rejected                                       |
| `create-es-api-key` without `--expiration`        | `--expiration never`; the flag is now required |

New in 3.0.0:

- `--report-start` and `--report-end`, for an explicit reporting window
- `--whole-days`, for widening that window to whole days
- `--xlsx-file`, for Excel output
- `--anonymize`, for pseudonymized reports
- `--es-api-key` and `--es-username` for every Elasticsearch command,
  so an API key and basic authentication are both accepted everywhere
- optional Elasticsearch flags and `--hub` for `get-new-users`,
  for when each new user first started a server
- optional Prometheus flags for `get-new-activity`,
  for each session's peak memory
- `--limit` for `get-es-docs`

The reports' layout has also changed; generate one to see it.

## Changes to Figures

- The reporting window now excludes its end,
  so a snapshot or a new user exactly on the boundary between two windows
  is counted by only the later one.

- A document whose `meta.interval` is zero or negative
  is skipped rather than counted.

- `--duration` now subtracts elapsed time rather than wall-clock time,
  so a window spanning a daylight-saving transition
  no longer covers 23 or 25 hours instead of 24.

2.x recorded the push interval only when given `--metadata interval=...`.
Documents it pushed without one carry no `meta.interval`
and are skipped by `get-new-activity`, as they were in 2.x.
The command now reports how many it skipped on standard error,
rather than reporting an index of them as no activity.
