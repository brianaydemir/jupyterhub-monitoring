# Monitoring and Reporting on JupyterHub Usage

A CLI toolkit for monitoring a
[JupyterHub](https://jupyterhub.readthedocs.io/) instance
and reporting on its usage.
It snapshots the live server list into
[Elasticsearch](https://www.elastic.co/elasticsearch),
reports on the accumulated history,
and can deliver those reports by email.

The general workflow is:

1. Run `push-servers` on a schedule
   to snapshot the live server list into Elasticsearch.

2. Run `get-new-activity` or `get-new-users` to generate reports.

The remaining commands
(`create-es-api-key`, `delete-es-api-key`, `list-es-api-keys`, `get-es-docs`)
are operational helpers for managing Elasticsearch access.

## Prerequisites

- Python ≥ 3.14
- [Poetry](https://python-poetry.org/) (for installing from source)
- Docker (optional, for running via container)

Python 3.14 is a hard requirement, not a preference:
the code uses
[PEP 758](https://peps.python.org/pep-0758/)
`except` clauses, which earlier interpreters cannot parse.

## Installation

### From Source

```
git clone https://github.com/brianaydemir/jupyterhub-monitoring.git
cd jupyterhub-monitoring
make init       # runs: poetry install && poetry run pre-commit install
```

The commands are then available through Poetry:

```
poetry run push-servers --help
```

### Docker Image

Build the image with:

```
make build IMAGE=myregistry.example.com/me/jupyterhub-monitoring
```

`IMAGE` selects the registry and image name;
it defaults to this project's own registry,
so set it to build into a different one.
The image is tagged with the version from `pyproject.toml`,
not with `latest`.
Run a command inside the container by passing it as the command:

```
docker run --rm myregistry.example.com/me/jupyterhub-monitoring:3.0.0 \
    push-servers --help
```

The image runs as an unprivileged user,
so pass credentials through the environment
rather than expecting a writable filesystem:

```
docker run --rm \
    -e JUPYTERHUB_API_KEY \
    -e ELASTICSEARCH_API_KEY \
    myregistry.example.com/me/jupyterhub-monitoring:3.0.0 \
    push-servers \
        --jupyterhub-endpoint https://hub.example.com/hub/api \
        --es-endpoint https://es.example.com:9200 \
        --es-index jupyterhub-servers \
        --interval 5m
```

To write report files instead, mount a directory the container can write to
(`-v "$PWD/out:/home/app/out"`) and point the output flags into it.

## Configuration

### JupyterHub API Token

Commands that talk to JupyterHub require an API token
with the following scopes:

```
list:users  read:users  read:servers
```

Pass the token via a file:

```
--jupyterhub-api-key /path/to/jh-api-key.txt
```

or via the environment variable `JUPYTERHUB_API_KEY`.
Use `--jupyterhub-ca-cert` to supply a CA certificate for TLS verification.

### Elasticsearch API Key

Commands that talk to Elasticsearch require an API key.
Pass it via a file:

```
--es-api-key /path/to/es-api-key.txt
```

or via the environment variable `ELASTICSEARCH_API_KEY`.
Use `--es-ca-cert` to supply a CA certificate for TLS verification.

As an alternative to an API key,
pass `--es-username USERNAME` to authenticate with a username and password;
the password is prompted for interactively.
`--es-api-key` and `--es-username` are mutually exclusive,
but `ELASTICSEARCH_API_KEY` and `--es-username` are not:
an explicit `--es-username` takes precedence over the environment variable,
so a container with the secret injected can still be used interactively.

Use `create-es-api-key`, `list-es-api-keys`, and `delete-es-api-key`
to manage Elasticsearch API keys (see below).

## External Services

- **JupyterHub** (API endpoint + API token) —
  required for `push-servers` and `get-new-users`

- **Elasticsearch** (API endpoint + API key or credentials) —
  required for `get-new-activity` and all `*-es-*` commands;
  required for `push-servers` unless `--debug` is given;
  optional for `get-new-users`

- **SMTP relay** —
  required only when using `--send-email`

## Commands

All commands accept `--help` for the full list of options.

### `push-servers`

Fetches the current server list from JupyterHub
and pushes each server as a timestamped document to an Elasticsearch index.
Intended to run on a schedule
(e.g., every few minutes via cron or a Kubernetes CronJob)
to build a history of server activity.

```
push-servers \
    --jupyterhub-endpoint https://hub.example.com/hub/api \
    --jupyterhub-api-key ~/secrets/jh-api-key.txt \
    --es-endpoint https://es.example.com:9200 \
    --es-api-key ~/secrets/es-api-key.txt \
    --es-index jupyterhub-servers \
    --interval 5m \
    --metadata hub=production
```

Each document uses flattened dotted-key paths
(e.g., `server.ready`, `meta.hub`)
combining the JupyterHub server object
with metadata and a timestamp.

Documents are pushed in batches through the Elasticsearch `_bulk` API,
so a hub with hundreds of servers costs a handful of requests
rather than one request per server.
Each server is still reported individually:
a document Elasticsearch rejects is counted and named on standard error,
and the rest of its batch is still pushed.

`--interval` records the push cadence
so that `get-new-activity` can compute per-user uptime.
It is always required,
and its value must match how often the command actually runs.

A run that takes longer than its own `--interval`
says so on standard error without changing its exit code,
because slowness is not a failed push.
Read that warning:
it means the next scheduled run may start before this one finishes,
and two overlapping runs stamp two snapshot times on one real interval,
which inflates every duration `get-new-activity` reports.

`--metadata KEY=VALUE` (repeatable) tags documents
with additional identifying context;
both report commands can filter on `meta.hub` via `--hub`.
The keys `interval`, `snapshot-time`, and `snapshot-time-iso` are reserved,
because the command sets them itself.

Use `--debug` to print documents to stdout
instead of pushing them to Elasticsearch,
and `--limit N` to process only the first N servers.

### `get-new-activity`

Queries Elasticsearch for server-snapshot documents in a time window
and reports on the server activity they describe.

```
get-new-activity \
    --es-endpoint https://es.example.com:9200 \
    --es-api-key ~/secrets/es-api-key.txt \
    --es-index jupyterhub-servers \
    --duration "7 days" \
    --hub production \
    --text-file activity.txt \
    --html-file activity.html
```

The report has up to four parts,
every one of which states `Times are HH:MM.` in a footnote
wherever it shows a duration,
so that the unit is given once per table
rather than carried by a column header:

- **Summary** —
  aggregate statistics for the window, in this order:
  the length of the reporting window;
  total active time;
  active users and sessions;
  GPU active time, its share of the total, and CPU active time;
  GPU users and GPU sessions;
  mean, median, and max time per user;
  mean sessions per user and mean session length;
  peak concurrent servers,
  mean concurrent servers (active),
  and mean concurrent servers (window);
  and the busiest day and that day's active time.
  Every row states a single quantity,
  so that a spreadsheet can sort or chart any one of them:
  the GPU share of the total is its own row rather than
  a percentage glued to the GPU time,
  and the busiest day's total is its own row
  rather than a time in parentheses after the date.
  When no day of the window saw any activity,
  both busiest-day rows read `n/a`,
  rather than one of them naming a duration
  for a day the report cannot name.
  The reporting window's length leads the table,
  in the same `HH:MM` units as the durations below it,
  so that a seven-day window reads as `168:00`
  and the totals have a denominator to be read against.
  The section's own sentence repeats that length,
  as in `Usage from 2026-09-02 00:00 to 2026-09-03 00:00 (24:00).`
  Peak and mean concurrency (active) are measured per hub push:
  documents are grouped by `meta.hub` and `meta.snapshot-time` together,
  since `push-servers` stamps one snapshot time on a whole run,
  so peak is the busiest single push by one hub
  rather than a cluster-wide total,
  and two hubs whose pushes land on the same second stay apart.
  Mean concurrency (active) averages those pushes,
  skipping the ones with no active server,
  so it describes the hub while it is in use
  and says nothing about how much of the window was idle.
  Mean concurrency (window) is instead
  the total active time divided by the length of the reporting window,
  so it counts the idle stretches too:
  it is the figure comparable between reporting windows,
  and the one to read as sustained load.
  A footnote defines both;
  when the documents name more than one hub,
  a further footnote names those hubs
  and repeats which figures are per hub push.

- **Activity by Day** —
  active server time bucketed by calendar day.
  Every calendar day of the reporting window has a row,
  so the series is contiguous:
  a day that saw no activity reads as `0:00`
  rather than dropping out from between the days around it.
  The window is half-open,
  so the last row is the day holding the window's last instant,
  which means a window ending exactly at local midnight
  gets no row for the day beginning there.
  Days are stepped through as calendar dates,
  so a daylight-saving transition neither skips nor repeats one.
  The section is omitted only when the window holds no day at all,
  as one of no length or an inverted one does.

- **User Totals** —
  one row per user, most active first:
  the institution, the identifier,
  the number of sessions the user held,
  their total active time, and the GPU part of that time.
  A user's session count includes the spawn-only sessions,
  so it matches the rows `User Activity` shows for them.
  The row count, the session count, and the two totals
  reconcile with the Summary,
  and the first row's time is its `Max time (one user)`,
  up to the truncation each duration is displayed with independently.
  Users with equal totals fall in order
  by institution and then by the identifier shown,
  so `--anonymize` orders on the pseudonym rather than on the raw ID.
  `User Activity` lists the same users in this same order.
  The `Login method` column appears here
  exactly when it appears in `User Activity`.
  A window that saw no activity keeps the section,
  showing its headers and no rows,
  as the per-session table does.

- **User Activity** —
  one row per *session*, where a session is a user,
  one of that user's named servers,
  and that server's `server.started` timestamp,
  so concurrent named servers and separate runs are tallied independently.
  Snapshots taken while a server is still spawning carry no
  `server.started`, so a user's spawn attempts on one named server
  share a single session, shown with a `Started` of `n/a`.
  Because `Started` identifies the session rather than merely dating it,
  it always carries the time of day, `YYYY-MM-DD HH:MM`,
  and this command takes no `--date-format`:
  a bare date would run a user's separate same-day runs
  of a single server together.
  A `Server` column names the server, immediately after `Started`,
  so that the two columns identifying the session sit together
  ahead of the ones identifying the user.
  The column appears only when some session ran on a named server:
  a report holding nothing but default servers omits it,
  since every row would otherwise read the same.
  Where it does appear,
  the default server reads as `(default)`,
  not as a blank cell that would look like missing data,
  and a document that named no server at all reads as `n/a`.
  Without the column,
  two of one user's concurrent servers render as identical rows.
  Users appear in the order `User Totals` lists them in,
  most active first,
  so the two tables read as one:
  a heavy user found in `User Totals`
  is found in the same place here,
  with that user's sessions detailed under them.
  Within one user, sessions read in start-time order,
  the ones with no `Started` last,
  and the named server breaks a tie between two starting together.
  The `GPU` column reports whether the session requested a GPU:
  a `server.user_options` entry counts
  when its value contains the whole word "gpu" or "gpus"
  anywhere, delimited by non-letters
  (for a list, when one of its elements does),
  or when its *key* names that word
  and the value is not switched off.
  So `cpu_and_gpu` counts, but `nogpu` and
  an unchecked `gpu: false` box do not.
  Neither does anything to tell a request from a mention,
  so a `tensorflow-gpu` image also counts as a GPU request.

A document contributes to the report when its `meta.snapshot-time`
falls in the half-open window `[start, end)`
and `server.ready == true` or `server.pending == "spawn"`;
each such document adds its `meta.interval` value to its session's total.
The window excludes its own end
so that back-to-back reports do not both count
the snapshot on their shared boundary.
Documents missing `user.name` or `meta.interval`
(e.g., those pushed before `--interval` was introduced),
and those whose `meta.interval` cannot be parsed
or is not positive,
are skipped,
and are excluded from every statistic in the report.
Documents where `meta.testing` is `"true"` or `true`
are automatically excluded.

Those skips are not silent.
Alongside the reporting window,
the command echoes to standard error
how many documents the query returned,
how many of them it counted,
and how many it skipped under each reason:

```
Retrieved 1440 documents; counted 1440.
Retrieved 1440 documents; counted 0, skipped 1440 (no meta.interval: 1440).
Retrieved 0 documents.
```

The reasons are `no user.name`, `no meta.interval`,
`unparsable meta.interval`, and `non-positive meta.interval`.
`push-servers` rejects a non-positive `--interval`,
so a document skipped for that reason was written by something else.

> [!TIP]
> Read that line too.
> A report of no activity looks the same
> whether the hub was idle,
> `--es-index` named the wrong index,
> `--hub` matched no document,
> or every document retrieved was rejected by the rules above.
> The tally separates the last of those from the rest,
> since it alone retrieves documents and then discards them,
> and `Retrieved 0 documents.` narrows the other three
> to the query rather than the data.

### `get-new-users`

Fetches the user list from JupyterHub
and reports on accounts created within the reporting window.
A user counts when their `created` timestamp falls in the half-open window
`[start, end)`, matching `get-new-activity`,
so back-to-back reports do not both count a user created on their boundary.
A user whose `created` timestamp is missing or unparsable is skipped.

```
get-new-users \
    --jupyterhub-endpoint https://hub.example.com/hub/api \
    --jupyterhub-api-key ~/secrets/jh-api-key.txt \
    --duration "7 days" \
    --csv-file new-users.csv
```

Alongside the reporting window,
the command echoes to standard error
how many user records the hub answered with
and how many of them fall in the window:

```
Retrieved 812 users; 3 created in the reporting window.
Retrieved 0 users.
```

> [!TIP]
> Read that line too.
> A report of no new users looks the same
> whether the window was quiet
> or the endpoint and token reached a hub with few or no users on it.
> The first number tells those apart;
> a hub that answered with nothing says only `Retrieved 0 users.`,
> since none of nothing can have been created in the window.

Elasticsearch is optional here.
Supplying `--es-endpoint` and `--es-index`
adds a **First server** column
reporting when each new user first started a server;
without them the column is omitted.
The time comes from the earliest `server.started` recorded for that user,
falling back to the snapshot time
on documents that carry no `server.started`
(which is late by at most one `--interval`).
A user with no matching snapshot is shown as `n/a`.
Pass `--hub` to scope the lookup to one hub's documents,
as `get-new-activity` does;
without it, a multi-hub index draws from every hub.
Because `--hub` scopes only this lookup,
passing it without `--es-endpoint` is an error.
The search runs from the start of the reporting window through now,
since a new user cannot have started a server before being created:

```
get-new-users \
    --jupyterhub-endpoint https://hub.example.com/hub/api \
    --jupyterhub-api-key ~/secrets/jh-api-key.txt \
    --es-endpoint https://es.example.com:9200 \
    --es-api-key ~/secrets/es-api-key.txt \
    --es-index jupyterhub-servers \
    --duration "7 days"
```

Those same two options also add an **Adoption** section
ahead of the **New Users** table,
answering how many of the new users went on to use the hub:

- **New users** —
  the number of rows in the table below it.

- **Started a server** —
  how many of those users the Elasticsearch data dates a first server for.

- **Never started a server** —
  the rest.
  The two always sum to **New users**.

- **Started same day** —
  how many started a server on the same calendar day they were created.
  The day is evaluated in `--timezone`,
  the zone the **Created** and **First server** columns are rendered in,
  so the count cannot contradict the dates the reader sees.

- **Median time to first server** —
  the median wait from `created` to that first server,
  over the users who started one.
  A first server dated before its own user counts as no wait at all
  rather than as a negative one:
  the snapshot fallback above, together with a search window
  that opens before the user was created, makes that possible.
  The row is omitted when no such wait could be measured,
  and the section's `Times are HH:MM.` footnote goes with it,
  since no cell would then be read in those units.
  The **New Users** table shows no duration, so it never carries one.

Without `--es-endpoint` and `--es-index` the section is omitted entirely,
since with no first server dated for anyone,
every figure in it would understate users
who may well have started servers.

### `create-es-api-key`, `delete-es-api-key`, `list-es-api-keys`

Create, invalidate, and list Elasticsearch API keys
owned by the authenticated user.

```
create-es-api-key \
    --es-endpoint https://es.example.com:9200 \
    --es-username elastic \
    --name push-servers-key \
    --expiration 30d \
    --format key

list-es-api-keys \
    --es-endpoint https://es.example.com:9200 \
    --es-username elastic

delete-es-api-key \
    --es-endpoint https://es.example.com:9200 \
    --es-username elastic \
    --name push-servers-key
```

`create-es-api-key` names the key `api-key-{username}` when
`--es-username` is given and `api-key` otherwise,
unless `--name` says otherwise.
It requires `--expiration`:
pass an Elasticsearch time value such as `30d`,
or `never` for a key that never expires,
so that a permanent credential is always a deliberate choice.
`delete-es-api-key` accepts either `--id` or `--name`.
`list-es-api-keys` shows active keys by default;
use `--all` to include expired and invalidated ones.
Both `create-es-api-key` and `list-es-api-keys`
accept `--format key` (default), `json`, or `full`.

### `get-es-docs`

Queries an Elasticsearch index with a Kibana-style query string
and prints each matching document as pretty-printed JSON.
The output is a stream of JSON objects rather than one JSON document,
so it is meant for reading, not for piping into a JSON parser.
Useful for ad-hoc inspection or debugging of the index.

```
get-es-docs \
    --es-endpoint https://es.example.com:9200 \
    --es-api-key ~/secrets/es-api-key.txt \
    --es-index jupyterhub-servers \
    --query "meta.hub:production AND server.ready:true"
```

Use `--limit N` to cap the number of documents retrieved.
Sorting is tied to `--limit`: with it, results come back
sorted descending by `meta.snapshot-time`, so the cap keeps the newest
documents; without it, they arrive in whatever order the index yields.

## Reporting Window

`get-new-activity` and `get-new-users` require exactly one of two window modes.

Look back a fixed amount of time from now:

```
--duration "7 days"     # also: "30 seconds", "15 min", "12h", "3d 6h 12m"
```

The duration is elapsed time, not wall-clock time,
so a window spanning a daylight-saving transition
is still exactly as long as it says.
It must be positive.

Or give an explicit range,
as human-readable datetimes:

```
--report-start "July 1 2026" --report-end "July 8 2026"
--report-start "2 weeks ago" --report-end "yesterday" --whole-days
```

The range must not end in the future.
`--report-end "now"` is allowed,
and so is anything within a minute of it,
but a value that resolves further ahead is rejected:

```
error: --report-end resolves to 2026-09-10 17:48, which is in the future
```

`--duration` needs no such rule,
because its window always ends
at the current moment
or at the most recent past occurrence of `--time`.

A bare date resolves to midnight,
but a relative phrase resolves to the current time of day:
run at 17:48, `--report-end "yesterday"` means yesterday at 17:48,
which leaves a ragged partial day at each end of the window.
`--whole-days` squares those edges off.
It moves `--report-start` back to the midnight that opens its day
and `--report-end` forward to the next midnight,
so that the half-open window covers in full every day it touches:

```
# Run at 2026-09-03 17:48, this window is
# 2026-08-20 00:00 to 2026-09-03 00:00, fourteen whole days.
--report-start "2 weeks ago" --report-end "yesterday" --whole-days
```

A bound already at midnight does not move,
so adding the flag to the `July 1 2026` range above changes nothing.
The two midnights are found by calendar date in `--timezone`,
so a window spanning a daylight-saving transition
still begins and ends at local midnight,
even though one of its days runs 23 or 25 hours.

An end whose next midnight has not yet arrived
moves back to its own midnight instead,
so that the window closes with the last day that finished
rather than reaching into one that has not.
`--report-end "now" --whole-days` therefore ends the window
at the midnight that opened today,
and never trips the future-end rule above;
`--report-end "tomorrow"` still lands ahead of the present
and is still rejected.

`--whole-days` applies only to `--report-start` / `--report-end`.
Combining it with `--duration`, which anchors its own end with `--time`,
is an error rather than a flag that quietly does nothing.

Whichever mode you use,
the resolved window is echoed to standard error before the report runs:

```
Reporting window: 2026-01-03 00:00 to 2026-09-03 17:48 (America/Chicago, 244 days)
```

The length reads in whole days,
or in `H:MM` hours for a window shorter than a day.

> [!TIP]
> Read that line.
> The datetimes are parsed generously,
> so a typo can still name a real date:
> `--report-start a` resolves to a date in January,
> and `--report-start 5` to one in May.
> The echo is what makes such a misreading visible.

With `--duration`, add `--time HH:MM`
to anchor the end of the window
to the most recent occurrence of that wall-clock time
within the past 24 hours,
rather than to the current moment.
`--time` cannot be combined with `--report-start` / `--report-end`.

`--timezone TZ` sets the timezone
for `--time`,
for interpreting `--report-start` / `--report-end` values
that carry no explicit offset,
and for all output timestamps.
It accepts IANA names (`America/Chicago`),
abbreviations that are valid IANA keys (`MST`),
and fixed offsets (`+04:00`, `-05:30`).
The default is `America/Chicago`.

> [!NOTE]
> Both report commands retrieve every matching snapshot document
> and aggregate them client-side,
> and `get-new-users` searches from the window start through now.
> A window far in the past, or a very long one,
> can therefore be expensive.

## Output Formats

`get-new-activity` and `get-new-users` always print the report to stdout,
and can additionally write it to any combination of
plain text (`--text-file`),
styled HTML (`--html-file`),
CSV (`--csv-file`),
and Excel (`--xlsx-file`) files,
and/or deliver it by email (`--send-email`).

`--csv-file` writes a single CSV document
containing every section, separated by blank rows.
A text value that begins with `=`, `+`, `-`, `@`,
a tab, or a carriage return is written with a leading apostrophe,
so a spreadsheet opening the file reads it as text
rather than as a formula.
`--xlsx-file` writes a workbook
whose first sheet, "Report", carries the title,
each section's heading and description, and every footnote,
followed by one sheet per table.
Each table sheet repeats its own section's notes below the table,
after a blank row,
so a sheet opened on its own still names
the timezone its timestamps are read in.
Its table cells carry numbers as numbers:
a count is a number, and a duration is an elapsed time
displayed as `[h]:mm`,
so the workbook sorts, sums, charts, and pivots them
instead of holding the text the other formats show.
Sorting a duration column therefore reads
`0:20`, `1:00`, `2:10`, `10:00`, `26:40`,
rather than the order the text of those values would give.
Cells that are text stay text,
so a value beginning with `=` is never read as a formula.
`--html-file` writes a standalone HTML document;
the email body uses the same markup as a fragment.

Sending email requires
`--sender-email`, `--recipient-email`, `--smtp-host`, and `--smtp-port`;
`--sender-name` and `--subject` are optional.
SSL/TLS is used by default; pass `--smtp-no-ssl` to disable it.

A report usually goes to more than one person,
so `--recipient-email` takes several recipients two ways:
an occurrence may list them, the way a `To:` header lists them,
and the flag may be repeated.
Both spellings mean the same thing, and may be mixed.

```
--recipient-email "Ops Team <ops@example.edu>, dean@example.edu" \
    --recipient-email provost@example.edu
```

A recipient is a bare address
or an address with a display name —
whatever a single mailbox may look like in that header.
A display name holding a comma has to be quoted,
as the header requires:
`"Doe, Jane" <jane@example.edu>`.
A quoted comma separates nothing,
so such a name is safe to write in a list.

Surrounding whitespace is ignored.
An occurrence naming no recipient is rejected,
as is one whose recipients are not all readable —
every address needs an `@`,
so a bare local name such as `root` is refused,
even where the relay would have delivered it.
Nothing else about an address is checked,
since the SMTP server is the authority on that.

A group, `Team: ops@example.edu, dean@example.edu;`, is also rejected.
That form names the group as well as its members,
and nothing here would carry the name,
so it is refused rather than silently reduced
to the list written without it.

Every recipient appears in the `To:` header,
and each is given to the server as a recipient of its own,
so a single message reaches all of them.
What appears there is the mailbox as re-rendered,
which is the same text for an ordinary address
but writes the older comment form `ops@example.edu (Ops Team)`
as `Ops Team <ops@example.edu>`.

`--subject` defaults to the report's name
followed by the days the report covers:

| Window       | Default subject                                         |
| :----------- | :------------------------------------------------------ |
| One day      | `JupyterHub Activity Report: 2026-09-02`                |
| Several days | `JupyterHub New Users Report: 2026-08-27 to 2026-09-02` |

A daily send is therefore told apart in an inbox list
without opening anything.
The reporting window is half-open,
so a window closing at midnight is named by the day it reported on
rather than by the day it stops at.
An explicit `--subject` is used exactly as given.

The report is sent as both plain text and HTML,
with the main data table attached as a CSV file
(`activity.csv` or `new-users.csv`).
The Summary and Activity by Day tables are not attached separately.
When `--xlsx-file` is also given, the workbook is attached as well.

`--date-format` selects how `get-new-users` renders its timestamps:
`date` (the default) is `YYYY-MM-DD`
and `datetime` is `YYYY-MM-DD HH:MM`.
Its `Created` and `First server` columns date a user,
so either rendering says the same thing about them.

`get-new-activity` does not take the option,
and always renders its timestamps as `YYYY-MM-DD HH:MM`.
Its `Started` column *identifies* a session rather than dating one,
so a bare date would render a user's separate same-day runs
of a single server as rows differing only in a duration.
The `Server` column cannot make up for that,
since both such rows name the same server.

Pass `--anonymize` to replace within-institution identifiers
with stable, generic pseudonyms drawn from the NATO phonetic alphabet.
The institution is still shown.
The same user maps to the same pseudonym across runs,
whatever else the report contains,
and the same identifier at two institutions maps independently.

Each label is three words, so there are 26³ = 17,576 of them
per institution, and two users at one institution can collide:
roughly 1% at 20 users, 7% at 50, and 24% at 100.

> [!IMPORTANT]
> The pseudonyms are a display convenience, not a privacy guarantee.
> They come from an unsalted, unkeyed hash of the username,
> so anyone holding the original usernames can recover the mapping
> by hashing their own candidates.

## Username Parsing

JupyterHub usernames encode the authentication source.
The report commands parse them to populate
the **Institution** and **ID** columns,
and, conditionally, a **Login method** column.

| Username format             | Institution   | ID                    | Login method |
| :-------------------------- | :------------ | :-------------------- | :----------- |
| `user@example.edu`          | `example.edu` | `user`                | local NetID  |
| `eppn:user@example.edu`     | `example.edu` | `user`                | legacy eppn  |
| `email:user@example.edu`    | `example.edu` | `user`                | email        |
| `orcid:0009-0008-3064-0494` | `orcid.org`   | `0009-0008-3064-0494` | ORCID        |
| anything else               | `(none)`      | the name as given     | `(none)`     |

A name in none of these forms carries
neither an institution nor a login method,
so both columns read `(none)`
rather than showing a blank cell that would look like missing data.
`--anonymize` changes only the **ID** column,
so the marker reads the same either way.
Such a name also sorts last in every table,
after even the ORCID rows,
since it is the least informative row the table holds.

The **Login method** column appears only when it is needed:
when `--detailed-usernames` is given,
or when two users share an (Institution, ID) pair.

## Exit Codes

| Code | Meaning                                                         |
| :--- | :-------------------------------------------------------------- |
| 0    | Success                                                         |
| 1    | The command ran but failed (see below)                          |
| 2    | Invalid command-line arguments, reported by the argument parser |

A command exits 1 when a service call fails,
when a credential is missing or unreadable,
when `push-servers` could not push at least one document,
or when `--send-email` could not deliver the report.
An exit of 1 from a report command does not mean nothing happened:
files requested with `--text-file` and friends
are written before the email is sent.

## Migrating from 2.x

Release 3.0.0 renames the Elasticsearch flags,
removes the standalone `send-email` command and `--recipient-name`,
and reorganizes the Python package.

| 2.4.0                              | 3.0.0                                                    |
| :--------------------------------- | :------------------------------------------------------- |
| `send-email` command               | (none; the report commands' `--send-email` is unchanged) |
| `send-email --attachment`          | (none; attachments are automatic)                        |
| `--recipient-email` (one address)  | one or more; list them or repeat it                      |
| `--recipient-name`                 | (none; write it in `--recipient-email`)                  |
| `--recipient-email root`           | rejected; an address needs an `@`                        |
| `--elasticsearch-endpoint`         | `--es-endpoint`                                          |
| `--elasticsearch-api-key`          | `--es-api-key`                                           |
| `--elasticsearch-ca-cert`          | `--es-ca-cert`                                           |
| `--elasticsearch-index`            | `--es-index`                                             |
| `--elasticsearch-username`         | `--es-username`                                          |
| `push-servers` without an interval | `--interval` now required                                |
| `--report-end` in the future       | rejected; the window must end by now                     |
| `app.<command>`                    | `app.cli.<command>`                                      |
| `app.elasticsearch_client`         | `app.clients.elasticsearch_client`                       |
| `app.jupyterhub_client`            | `app.clients.jupyterhub_client`                          |
| `app.name_utils`                   | `app.core.name_utils`                                    |
| `app.time_utils`                   | `app.core.time_utils`                                    |
| `app.send_email`                   | `app.reports.email_utils`                                |

Some modules were split rather than renamed:

- The report and push commands kept their entry points at
  `app.cli.<command>`,
  but their analytics moved to `app.usage`:
  `app.usage.snapshots` (the server-snapshot query and accessors),
  `app.usage.activity` (per-session accounting),
  `app.usage.new_users` (new-user selection), and
  `app.usage.push` (the push pipeline).

- Each command's own `parse_arguments`, and the helpers in
  `app.cli_utils` that it called, became `app.cli`:
  `app.cli.arguments` (the argument groups),
  `app.cli.validation` (post-parse validation),
  `app.cli.report_arguments` (the argument groups and validation
  the report commands share),
  `app.cli.credentials` (prompting for a password and reading an API key),
  `app.cli.factories` (the clients and values built from parsed
  arguments), and
  `app.cli.runtime` (the parse/validate/run wrapper).

- `JupyterHubClient._flatten_dict` left `app.jupyterhub_client` for
  `app.core.dict_utils`, which holds it as the public, generic
  `flatten_dict`.

- The `format_users_*` and `format_activity_*` functions in
  `app.output_formatters` became `app.reports`:
  `app.reports.model` (the report types),
  `app.reports.builders` (constructing a report),
  `app.reports.renderers` (rendering one, now also as XLSX), and
  `app.reports.delivery` (writing and sending it).

- `app.reports.anonymize` (the `--anonymize` pseudonyms),
  `app.core.errors` (the exception types the clients now raise,
  in place of the built-in `ValueError` and `ConnectionError`), and
  `app.core.dataclass_defaults` (typed `default_factory` helpers)
  are new modules with no 2.x counterpart.

New in 3.0.0:
`--report-start` / `--report-end` for an explicit reporting window,
`--whole-days` for rounding that window out to whole days,
`--xlsx-file` for Excel output,
`--anonymize` for pseudonymized reports,
and `--hub` on `get-new-users`,
whose first-server lookup previously drew from every hub in the index.

Activity figures also shift slightly.
The reporting window now excludes its own end,
so a window reports one interval less than it did in 2.x;
sessions are keyed by named server as well as start time;
a `server.user_options` key naming a GPU
counts only when its value is switched on;
and a value counts only when it leads with "gpu" or "gpus",
rather than whenever it names that word anywhere.

The activity report's shape changed too.
2.x printed a single table of users and their total times,
under a sentence giving the window's bounds.
3.0.0 replaces that with four sections —
Summary, Activity by Day, User Totals, and User Activity —
so most of the report has no 2.x counterpart,
including every row of the Summary:
the reporting window's length,
the totals and averages,
and the three concurrency figures.
The `get-new-activity` section above describes each of them.

Two window boundaries changed as well.
`--duration` now subtracts elapsed time rather than wall-clock time,
so a window spanning a daylight-saving transition
no longer covers 23 or 25 hours instead of 24.
`get-new-users` now excludes the end of its window,
which it previously included.

Documents pushed by 2.x carry no `meta.interval`
and are skipped by `get-new-activity`,
so activity reports only cover the period
since `--interval` started being recorded.
The command's document tally on standard error
now names those skips,
rather than reporting an index of them as no activity.

## License

Apache License 2.0. See [LICENSE](LICENSE).
