# Agent Instructions

This file captures project-specific guidance for working in this repository.

## Commands

```bash
make all        # Run tidy, lint, and build (default target)
make init       # Install dependencies and pre-commit hooks
make update     # Upgrade dependencies and report outdated packages
make tidy       # Normalize formatting and spelling
make lint       # Surface static analysis and quality issues
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
  command entry points, shared runtime, and utilities

- `app/clients` —
  thin wrappers around the JupyterHub and Elasticsearch APIs

- `app/core` —
  shared domain utilities

- `app/reports` —
  report models, builders, renderers, and delivery helpers

The full set of available commands is defined in `pyproject.toml`,
in the `[project.scripts]` section.

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

There is currently no automated test suite in this repository,
so this workflow ends with file-level pre-commit checks.

1. `make tidy` —
   normalize formatting and spelling

2. `make lint` —
   surface static analysis and quality issues

   **Note:**
   `make lint` always runs all linters, even if one exits non-zero,
   because each command in the `lint` target is prefixed with `-`.

3. `poetry run pre-commit run --files <changed files>` —
   run pre-commit checks on the changed files

   **Note:**
   Some hooks edit files automatically.
   If any files are modified,
   rerun this step until it passes cleanly.

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
