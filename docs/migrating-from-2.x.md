# Migrating from 2.x

Release 3.0.0 renames the Elasticsearch flags
and removes the standalone `send-email` command and `--recipient-name`.

## Commands and Flags

| 2.4.0                                      | 3.0.0                                                    |
| :----------------------------------------- | :------------------------------------------------------- |
| `send-email` command                       | (none; the report commands' `--send-email` is unchanged) |
| `send-email --attachment`                  | (none; attachments are automatic)                        |
| `--recipient-email` (one address)          | one or more; list them or repeat it                      |
| `--recipient-name`                         | (none; write it in `--recipient-email`)                  |
| `--recipient-email root`                   | rejected; an address needs an `@`                        |
| email flags without `--send-email` ignored | rejected                                                 |
| `--elasticsearch-endpoint`                 | `--es-endpoint`                                          |
| `--elasticsearch-api-key`                  | `--es-api-key`                                           |
| `--elasticsearch-ca-cert`                  | `--es-ca-cert`                                           |
| `--elasticsearch-index`                    | `--es-index`                                             |
| `--elasticsearch-username`                 | `--es-username`                                          |
| `--metadata interval=5m`                   | `--interval 5m`, now required                            |
| reserved `--metadata` keys ignored         | rejected                                                 |
| `create-es-api-key` without `--expiration` | `--expiration never`; the flag is now required           |

New in 3.0.0:

- `--report-start` and `--report-end`, for an explicit reporting window
- `--whole-days`, for widening that window to whole days
- `--xlsx-file`, for Excel output
- `--anonymize`, for pseudonymized reports
- optional Elasticsearch flags for `get-new-users`
- optional Prometheus flags for `get-new-activity`,
  for each session's peak memory

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
