# Copilot Instructions

## Commands

```bash
make init       # Install dependencies (poetry install)
make reformat   # Format code with mdformat, isort, and black
make lint       # Run bandit, mypy, and pylint
make build      # Clean dist/, build Python wheel, build Docker image
make clean      # Remove dist/, docs/_build/, and docs/api/
make distclean  # Remove all untracked/ignored files except .python-version
make docs       # Build Sphinx HTML documentation
make update     # Update dependencies and regenerate requirements.txt
make all        # reformat + lint + build
```

There are no automated tests.
Linters run with `-` prefix (non-fatal) in the Makefile.

Pre-commit hooks can be run manually with:

```bash
pre-commit run --all-files
```

## Architecture

This project provides CLI scripts that pull data from a JupyterHub instance
and push it to Elasticsearch,
plus utilities for managing Elasticsearch API keys and sending emails.

**Two client wrappers** in `app/`:

- `JupyterHubClient` — wraps JupyterHub REST API;
  `list_servers()` returns flattened dicts with dot-notation keys
  (e.g., `user.name`, `server.state`)
- `ElasticsearchClient` — wraps the official Python client;
  constructor uses API key auth,
  class methods for API key management use basic auth via HTTP requests directly

**Utility modules** in `app/`:

- `cli_utils.py` — reusable argument-group builders and validators used by
  all CLI scripts (JupyterHub, Elasticsearch, query, output, and email groups)
- `name_utils.py` — helpers for parsing and normalizing JupyterHub usernames
- `output_formatters.py` — text, HTML, and CSV formatters for user lists and
  activity reports
- `time_utils.py` — helpers for time range computation and timezone parsing

**CLI scripts** map to `[project.scripts]` entries in `pyproject.toml`.
They fall into four categories: data pipeline (JupyterHub → Elasticsearch),
reporting (user lists and activity reports),
Elasticsearch API key management,
and email.
See `pyproject.toml` for the current list.

**Deployment**: `make build` cleans `dist/`,
builds a Python wheel with `poetry build`,
and then builds a Docker image from the installed wheel.

## Conventions

**Script structure**: Every CLI script follows:
argparse argument parsing in `parse_arguments()`,
`main()` returning `int` exit code,
errors printed to `sys.stderr`.

**API key input**: Secrets are accepted as either a file path argument
(`--*-api-key`)
or an environment variable (`JUPYTERHUB_API_KEY`, `ELASTICSEARCH_API_KEY`).
File contents are `.strip()`ped.

**CA certificates**: All external connections accept an optional
`--*-ca-cert` path argument for TLS verification.

**Document field naming**: Elasticsearch documents use dot-notation keys.
Metadata added by scripts uses the `meta.` prefix
(e.g., `meta.snapshot-time`).
The keys `snapshot-time` and `snapshot-time-iso` are reserved.

**`[project.scripts]` entries must be in alphabetical order.**

**Python version**: Requires Python ≥ 3.14
(uses union type syntax `X | Y`, `match` etc.).

**Formatting**: black with line-length 88, isort with `profile = "black"`.

**Pre-commit**: The repo uses `pre-commit`; see `.pre-commit-config.yaml` for
the full list of hooks.
Run `pre-commit run --all-files` to check everything at once.

## Commit Preferences

- **Subject line**: Short imperative phrase,
  no trailing period
  (e.g., `Add --subject argument to send-email`)
- **Body**: Wrap at ~72 characters;
  use `-` bullet lists for multi-item changes
- **Workflow**: Never commit automatically or speculatively.
  Only create a commit if the user has just explicitly asked you to commit.
  Always run `make reformat && make lint` before staging.
  Stage changes with `git add` and ask the user to review before committing.
- **Planning**: The final step of every implementation plan must be to ask
  the user to review the staged changes before committing.
- **Authorship**: Unless told otherwise,
  Copilot should be the commit author
  and the user should be left as the committer.
  Use `--author` to set the author to
  `Copilot <223556219+Copilot@users.noreply.github.com>`
  and include the trailer
  `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`
- **Dependencies**: Pin to the latest minor release at update time
  using `~X.Y` (e.g., `humanize = "~4.15"`)
