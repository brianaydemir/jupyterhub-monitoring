# Server Snapshots

`push-servers` records the hub's servers in Elasticsearch,
and the report commands read those records back.
This page describes what is recorded and how it is counted.

## Documents

Each run of `push-servers` writes one document per server.
A document is the server's JupyterHub model,
flattened to dotted keys,
plus metadata about the run:

- `user.name` —
  the server's owner

- `server.*` —
  the server's own fields,
  such as `server.name` (empty for the default server),
  `server.ready`, `server.started`, and `server.user_options.*`

- `meta.interval` —
  the run's `--interval`, as given (for example, `5m`)

- `meta.snapshot-time` and `meta.snapshot-time-iso` —
  the time of the run, in Unix seconds and in ISO 8601;
  every document from one run carries the same value

- `meta.KEY` —
  one for each `--metadata KEY=VALUE`

Tag each hub's documents with `--metadata hub=NAME`;
the report commands' `--hub NAME` selects them,
matching the name exactly.
The reports ignore documents tagged `--metadata testing=true`.

## Active Time

`get-new-activity` reads each document
as one observation of a server
that was active for that document's `meta.interval`.
A document counts when:

- its `meta.snapshot-time` falls in the reporting window,
  which includes its start and excludes its end; and

- its server is running (`server.ready` is true)
  or starting (`server.pending` is `spawn`).

Each document that counts
adds its `meta.interval` to its server's active time.
Two rules follow:

- `--interval` must match how often `push-servers` actually runs.
  Running every five minutes with `--interval 10m`
  doubles every figure.

- Runs must not overlap.
  Two runs in progress at once both record the same servers,
  so that time is counted twice.
  `push-servers` warns on standard error
  when a run takes longer than its `--interval`.

A document without a `user.name`,
or without a `meta.interval` that parses as a positive duration,
is skipped.
`get-new-activity` prints on standard error
how many documents it retrieved, counted, and skipped,
with the skips broken out by reason.
