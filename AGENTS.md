# Agent Instructions

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

This project is a CLI toolkit
for JupyterHub monitoring with the following layers:

- `app/cli/*` —
  command entry points and shared argument/validation runtime

- `app/clients/*` —
  thin wrappers around JupyterHub and Elasticsearch APIs

- `app/reports/*` —
  report model, builders, renderers, and delivery

- `app/core/*` contains shared domain utilities
  (errors, time handling, username parsing/sorting)

The authoritative command surface is `[project.scripts]` in `pyproject.toml`.

## Conventions

Authoritative tool configuration is in `pyproject.toml`.

This repository uses `pre-commit`.

`[project.scripts]` entries must be in alphabetical order.

### Docstring preferences

- Use Google-style docstring formatting.

- Keep prose as concise as possible.
  Assume that readers have the code in front of them,
  and
  only call out interesting or non-obvious details.

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
   run linter;
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
  Include a concise description of the main features
  and architectural changes being made.
  Include the rationale behind those changes when known.

  Keep body lines under 76 characters where reasonable.

- Authorship:
  The agent should be the commit's author
  unless told otherwise.

  Use `--author` to set the author to the agent's identity.
  The following table provides common identities for attribution:

  | Agent          | Name                      | Email                                                             |
  | :------------- | :------------------------ | :---------------------------------------------------------------- |
  | Gemini         | `gemini-code-assist[bot]` | `176961590+gemini-code-assist[bot]@users.noreply.github.com`      |
  | GitHub Copilot | `Copilot`                 | `223556219+Copilot@users.noreply.github.com`                      |
  | OpenAI Codex   | `chatgpt-codex-connector` | `199175422+chatgpt-codex-connector[bot]@users.noreply.github.com` |

  Include the trailer `Co-authored-by:` with the same author identity.

  The user should be left as the committer.

- Signing:
  If `git commit` fails due to a signing error,
  retry with `--no-gpg-sign`.
