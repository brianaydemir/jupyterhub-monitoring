# Agent Instructions

This file captures project-specific guidance for working in this repository.

## Commands

```bash
make all        # Run tidy, lint, test, and build (default target)
make init       # Install dependencies and pre-commit hooks
make update     # Upgrade dependencies and report outdated packages
make tidy       # Normalize formatting and spelling
make lint       # Surface static analysis and quality issues
make test       # Run the test suite
make build      # Build the Python package and Docker image
make clean      # Remove Python package build artifacts
make distclean  # Remove untracked and ignored files, except .python-version
```

`make build` runs `docker build`,
so `make build` and `make all` both require Docker.

## Working with the User

**The user is always available to answer questions.**
Never resolve questions on their behalf.

Ask for clarification before proceeding whenever:

- the prompt has more than one plausible interpretation,

- the work requires information not present in the prompt, or

- multiple valid approaches exist with meaningfully different trade-offs.

Do not guess or pick an answer in these situations.

## Architecture

This project is a CLI toolkit for monitoring JupyterHub usage
and generating reports from that data.

It is organized into the following sub-packages:

- `app/cli` —
  command entry points,
  `runtime` (the parse/validate/run wrapper every command's `main` uses),
  and the helpers those commands share:
  `arguments` (the `add_*` argument builders, plus
  `configure_es_admin_parser`),
  `validation` (the post-parse validators, which report through
  `parser.error`),
  `report_arguments` (`ReportSource` and the whole-command
  `configure_report_parser` / `validate_report_arguments` pair),
  `credentials` (prompting for a password and reading an API key), and
  `factories` (the clients, `OutputTargets`, and other values built
  from parsed arguments;
  resolving the reporting window also echoes it to stderr,
  which is how both report commands get that echo without duplicating it)

  Imports among these flow one way:
  `arguments`, `validation`, `credentials`, and `runtime` depend on
  nothing else in `app/cli`;
  `factories` imports `credentials`;
  `report_arguments` imports `arguments` and `validation`;
  and the command modules import from every one of these
  except `credentials`, which they reach through `factories`.
  Keep it that way,
  rather than deferring an import inside a function to break a cycle.

- `app/clients` —
  thin wrappers around the JupyterHub and Elasticsearch APIs

- `app/core` —
  shared errors and utilities that are tied to no service:
  `errors` (the project's exception types),
  `dataclass_defaults` (the typed empty-collection factories that
  dataclass fields use as their `default_factory`), and
  `dict_utils`, `name_utils`, and `time_utils`

- `app/reports` —
  turning usage data into a delivered report:
  `model` (the rendering types),
  `builders` (assembling a report from `app/usage` data),
  `anonymize` (the pseudonyms `--anonymize` substitutes),
  `renderers` (text, HTML, CSV, and XLSX output),
  `delivery` (writing the files and sending the mail), and
  `email_utils` (composing and sending one message)

- `app/usage` —
  the JupyterHub usage domain:
  `snapshots` (the Elasticsearch query for active server documents,
  the exact `--hub` match its results go through,
  and the accessors for a snapshot's hub and timestamps),
  `activity` (per-session accounting, the report summary,
  and the tally of which documents could be counted),
  `new_users` (new-user selection, first-server times, and adoption), and
  `push` (the pipeline behind `push-servers`)

A client in `app/clients` raises the exception types in
`app/core/errors.py`, never its library's own.
Each method names the operation that failed
(for example, `Querying Elasticsearch failed: ...`),
so a command module can let the error travel to `run_command`
instead of translating it at its own boundary.

`ElasticsearchClient.upload_documents` is the one method
that does not report every failure that way.
A document Elasticsearch rejects is described
in that document's own result rather than raised,
which is what lets `push-servers` count it
without losing the rest of the batch;
only a failure of the request as a whole raises.

`app/usage` is named for the subject the project reports on,
not for any one report,
because it covers new users and the push pipeline
as well as activity accounting.
Its modules take plain data and return plain data,
which is what lets a command module in `app/cli` hold
little more than its docstring, `_build_parser`,
`_validate_arguments`, `_run`, and `main`.
The two report commands add a module-level `_SOURCES`,
which the parser and the validator share
so that the two cannot disagree about which arguments exist,
and a `_SUBJECT_PREFIX`,
which the parser and the run share
so that the default subject the help describes
is the one a message carries.
`get_new_users` adds `_query_first_server`,
whose search window is not the reporting window.
Each report command also holds the formatter for its own stderr tally,
`_format_document_tally` and `_format_user_tally`,
and the API-key commands hold their own output formatters;
these are presentation rather than business logic.
Put new JupyterHub or Elasticsearch business logic here,
not in a command module.

The domain types a report is built from,
`SessionActivity` and `ActivitySummary`,
sit in `app/usage/activity.py` beside the code that produces them.
`app/reports` imports from `app/usage`, never the reverse.

Report delivery is configured with the typed `OutputTargets` and
`EmailConfig` objects that `app/cli` builds from its parsed arguments,
so nothing under `app/reports` reads an `argparse.Namespace`.

Tests live in `tests`,
whose sub-packages mirror those of `app`,
so that a test module's path names the module it covers:
`tests/cli/test_validation.py` covers `app/cli/validation.py`.
Every directory carries an `__init__.py`,
which is what keeps two same-named test modules
in different sub-packages from colliding.
`tests/helpers` is the one directory that mirrors nothing;
it holds the stand-ins the tests share,
`clock` (a frozen `datetime`),
`sample_reports` (report values for the renderer and delivery tests), and
`stub_clients` (stand-in Elasticsearch clients for the command tests).

Most modules have a test module of their own.
These do not, and are covered through the code that uses them:

- `app/reports/email_utils.py` — `tests/reports/test_delivery.py`

- `app/core/errors.py` and `app/core/dataclass_defaults.py` —
  exercised by everything that raises or declares them;
  a test asserting that an empty exception subclass exists
  would assert nothing

- each `__init__.py` —
  a docstring, plus the re-exports in `app/core` and `app/clients`
  that are extension points (see below)

Give a module a test module of its own once it grows logic of its own,
rather than leaving that logic covered only through a caller.

The full set of available commands is defined in `pyproject.toml`,
in the `[project.scripts]` section.

Some of the code deliberately keeps surface that nothing currently calls:
`TextBlock`, `Report.footnotes`, and `ReportSection.attachments` in
`app/reports/model.py`,
the re-exports in `app/core/__init__.py` and `app/clients/__init__.py`,
`ElasticsearchClient.upload_document`,
the `state` parameter of `JupyterHubClient.list_users`,
and the `required` switch of `add_email_arguments`.
Each says so in its docstring.
Treat them as extension points, not as oversights to clean up.

The switches that `app/cli/arguments.py` and
`app/cli/report_arguments.py` thread through are all exercised,
and are not on that list:
`push-servers` and `OPTIONAL_ELASTICSEARCH_SOURCE` pass `required=False`,
`get-es-docs` passes `include_index=True`,
`get-new-activity` passes `include_date_format=False`,
and both report commands pass `include_anonymize=True`.

## Conventions

Follow the guidelines in this section when making changes,
and make changes only when there is a clear, objective reason.

### `pyproject.toml`

Keep entries in the following sections in alphabetical order:

- `[project.scripts]`
- `[tool.poetry.dependencies]`
- `[tool.poetry.group.dev.dependencies]`

**Note:**
In `[tool.poetry.dependencies]` only,
`python` is an exception and should always come first.

### Command-line flags

A flag that does nothing unless some other flag is also given
is an error when given without it,
rather than silently ignored:
for example, `--es-index` without `--es-endpoint`,
or `--subject` without `--send-email`.
Report it from a validator in `app/cli/validation.py`.

### Spacing

In comments and text files,
use a single space after a sentence-ending period
when more text follows on the same line.

### Line length

- For code, follow the line-length configuration in `pyproject.toml`.

- For comments and text files,
  keep lines under 80 characters, except for tables and embedded code.

### Line breaks

Use [Semantic Line Breaks](https://sembr.org/) in comments and text files.

- Avoid breaking within tight semantic units.

- Break at natural semantic boundaries, not at an arbitrary column.

### Prose style

- Use American English spellings.

- Use the Oxford comma in lists of three or more items.

- Use a consistent grammatical form
  for each item in lists and parallel constructions.

### Docstring preferences

- Use Google-style docstring formatting.

- Keep prose concise.
  Assume that the reader is reading the code at the same time,
  and highlight only interesting behavior.

- Apply the same conciseness principle to `Args` and `Returns`.
  Omit these sections when the function name and type annotations suffice.

- In `Raises`, list exceptions that callers should handle.
  Prefer precise exception types,
  but also avoid a long exhaustive list.

## Validation Workflow

After making changes, run the following steps
unless a step is clearly irrelevant.
After each step, address all warnings and errors before proceeding.

Documentation changes often still need step 1,
because `make tidy` formats Markdown
and runs spelling checks across the repository.

1. `make tidy` —
   normalize formatting and spelling

2. `make lint` —
   surface static analysis and quality issues

   **Note:**
   `make lint` always runs all linters, even if one exits non-zero,
   because each command in the `lint` target is prefixed with
   `$(IGNORE_ERRORS)`, which defaults to `-`.
   Pass `IGNORE_ERRORS=` to stop at the first failure,
   which is how CI invokes it.

3. `make test` —
   run the test suite

   **Note:**
   `pytest` is configured to turn warnings into errors,
   so a new `DeprecationWarning` fails the suite.
   Fix the cause, or add a narrow entry to `filterwarnings`
   in `pyproject.toml` explaining why it cannot be fixed.

4. `poetry run pre-commit run --files <changed files>` —
   run pre-commit checks on the changed files

   **Note:**
   Some hooks edit files automatically.
   If any files are modified,
   rerun this step until it passes cleanly.

Changes are also checked by `.github/workflows/ci.yml`,
which runs the same formatters, linters, and tests,
and verifies the built wheel's metadata.

## Commit Preferences

Never stage or commit
unless the user's most recent message explicitly asks you to.

When committing, follow these conventions:

- **Authorship:**
  Use `--author "Name <Email>"` to set the author to the agent's identity,
  unless the user specifies otherwise.

  The following table provides common identities for attribution:

  | Agent              | Name          | Email                                            |
  | :----------------- | :------------ | :----------------------------------------------- |
  | Claude             | `claude[bot]` | `209825114+claude[bot]@users.noreply.github.com` |
  | GitHub Copilot CLI | `Copilot`     | `223556219+Copilot@users.noreply.github.com`     |

  Leave the user as the committer.

- **Subject line:**
  Use a short imperative phrase with no trailing period.

- **Body:**
  Include a concise description of the changes.
  Include the rationale behind those changes when it is not self-evident.
  Follow the line length convention for text and comments.

- **Trailer:**
  Append the following to every commit message:

  ```
  Co-authored-by: Name <Email>
  ```

  Use the same `Name` and `Email` as the commit's author,
  as specified above.

- **Signature:**
  Retry with `--no-gpg-sign` if `git commit` fails due to a signing error.

**Note:**
`pre-commit` hooks also run automatically at commit time.
If a hook edits files, stage those changes and retry the commit.
If a hook fails, address the issue and retry the commit.
