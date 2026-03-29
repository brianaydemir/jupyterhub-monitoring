# Copilot Instructions

## Commands

```bash
make init       # Install dependencies
make update     # Update dependencies; show outdated packages
make tidy       # Tidy up files with mdformat, isort, black, and typos
make lint       # Run bandit, mypy, pyright, and pylint
make build      # Remove artifacts, build Python distribution, build Docker image
make docs       # Build Sphinx HTML documentation
make clean      # Remove artifacts
make distclean  # Remove all untracked/ignored files except .python-version
make all        # tidy + lint + build + docs
```

There are no automated tests.

Linters run with the `-` prefix (non-fatal) in `Makefile`.

## Architecture

This project provides CLI scripts for monitoring JupyterHub:

- `push-servers` —
  push server-state snapshots from JupyterHub to Elasticsearch

- `get-es-docs`,
  `get-new-activity`,
  `get-new-users` —
  query Elasticsearch;
  produce reports as text, HTML, and CSV

- `create-es-api-key`,
  `delete-es-api-key`,
  `list-es-api-keys` —
  manage Elasticsearch API keys

Two client wrappers are in `app`:

- `ElasticsearchClient` —
  wraps the official Python client

- `JupyterHubClient` —
  wraps the JupyterHub REST API;
  `list_servers()` returns flattened `dict`s with dot-notation keys
  (e.g., `user.name`, `server.state`)

Utility modules are in `app`:

- `cli_utils` —
  reusable argument-group builders,
  validators,
  and other CLI helpers (e.g., API key reading, duration parsing, credential prompting)

- `name_utils` —
  JupyterHub username parsing and sort-priority logic

- `report_model`, `report_builders`, `report_renderers`, `report_delivery` —
  report framework modules for structured sections,
  format rendering,
  and multi-destination delivery (stdout/files/email with attachments)

- `email_utils` —
  reusable SMTP email composition/sending helpers for report scripts

- `time_utils` —
  timezone parsing and time-range computation

CLI scripts map to `[project.scripts]` entries in `pyproject.toml`.

## Conventions

Python version:
Requires Python ≥ 3.14.

Formatting:
`black` with line-length 96,
`isort` with `profile = "black"`.

Pre-commit:
This repository uses `pre-commit`.

`pyright` runs in strict type-checking mode (`typeCheckingMode = "strict"`).

`mdformat` is configured by `.mdformat.toml`.

`typos` is configured by `.typos.toml`.

`[project.scripts]` entries must be in alphabetical order.

### Docstring preferences

- Use Google-style docstring formatting.

- Keep prose as concise as possible.
  Assume readers have the code in front of them,
  and only call out interesting or non-obvious details.

- Apply the same principle to `Args` and `Returns`.
  Omit those sections when behavior is sufficiently obvious.

- In `Raises`,
  list exceptions callers should reasonably catch and handle.
  Prefer concise, meaningful exception detail:
  avoid broad placeholders like `Exception`,
  but do not enumerate every leaf exception type.

## Post-Edit Workflow

After modifying files,
run the following steps in order:

1. `make tidy` —
   tidy up code and other files

2. `make lint` —
   run linters;
   address all warnings and errors before proceeding

3. `make docs` —
   build documentation;
   address all warnings and errors before proceeding.
   Note that `docs/api/` is auto-generated and should not be manually edited.

4. `poetry run pre-commit run --files <edited files>` —
   run pre-commit hooks on the files that were edited.
   If the hooks automatically fix any files,
   re-run the affected steps above before proceeding.

## Commit Preferences

- Staging and committing:
  Only stage or commit changes
  when the user's current message explicitly asks you to.
  Never stage or commit
  speculatively, automatically, or as part of an implementation workflow.

- Subject line:
  Short imperative phrase,
  no trailing period.

- Body:
  Include a concise description of the main feature
  and/or architectural changes being made.
  Include the rationale behind those changes when known.

- Authorship:
  Unless told otherwise,
  Copilot should be the commit's author.

  Use `--author` to set the author to
  `Copilot <223556219+Copilot@users.noreply.github.com>`.

  Include the trailer `Co-authored-by:` with the same author identity.

  The user should be left as the committer.
