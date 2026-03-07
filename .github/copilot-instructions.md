# Copilot Instructions

## Commands

```bash
make init       # Install dependencies
make update     # Update dependencies; show outdated packages
make reformat   # Format code with mdformat, isort, black, and typos
make lint       # Run bandit, mypy, pyright, and pylint
make build      # Clean artifacts, build Python distribution, build Docker image
make docs       # Build Sphinx HTML documentation
make clean      # Remove artifacts
make distclean  # Remove all untracked/ignored files except .python-version
make all        # reformat + lint + build + docs
```

There are no automated tests.
Linters run with `-` prefix (non-fatal) in the Makefile.
Run pre-commit hooks with `pre-commit run --all-files`.

## Architecture

This project provides CLI scripts for monitoring JupyterHub:

- pushing server-state snapshots from JupyterHub into Elasticsearch

- querying Elasticsearch to produce reports formatted as text, HTML, and CSV

- sending emails via SMTP

- managing Elasticsearch API keys

**Two client wrappers** are in `app/`:

- `ElasticsearchClient` — wraps the official Python client

- `JupyterHubClient` — wraps the JupyterHub REST API;
  `list_servers()` returns flattened dicts with dot-notation keys
  (e.g., `user.name`, `server.state`)

**Utility modules** are in `app/`:

- `cli_utils` — reusable argument-group builders and validators

- `name_utils` — JupyterHub username parsing and sort-priority logic

- `output_formatters` — produces text, HTML, and CSV report output

- `time_utils` — timezone parsing and time-range computation

**CLI scripts** map to `[project.scripts]` entries in `pyproject.toml`.

## Conventions

**Python version**: Requires Python ≥ 3.14.

**Formatting**: black with line-length 96, isort with `profile = "black"`.

**`pyright` runs in strict type-checking mode** (`typeCheckingMode = "strict"`).

**`[project.scripts]` entries must be in alphabetical order.**

**Pre-commit**: This repository uses `pre-commit`.

## Post-Edit Workflow

After modifying code files, run the following steps in order:

1. **`make reformat`** — auto-format all code.

2. **`make lint`** — run linters;
   address all warnings and errors before proceeding.

3. **`make docs`** — build documentation;
   address all warnings and errors before proceeding.

4. **`pre-commit run --files <edited files>`** —
   run pre-commit hooks on the files that were edited.
   If hooks auto-fix any files,
   re-run the affected steps above before proceeding.

## Commit Preferences

- **Staging and committing**:
  Only stage or commit changes
  when the user's current message explicitly asks you to.
  Never stage or commit
  speculatively, automatically, or as part of an implementation workflow.

- **Subject line**:
  Short imperative phrase, no trailing period

- **Authorship**:
  Unless told otherwise, Copilot should be the commit author.
  Use `--author` to set the author to
  `Copilot <223556219+Copilot@users.noreply.github.com>`
  and include the trailer
  `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`.
  The user should be left as the committer.
