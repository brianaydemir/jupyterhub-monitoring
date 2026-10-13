"""Obtaining the credentials a command authenticates with."""

import getpass
import os
import sys
from pathlib import Path

from app.core.errors import AppError, DataShapeError


def prompt_password() -> str | None:
    """Prompt for a password interactively.

    The username always arrives on the command line,
    so the password is the one credential ever prompted for.
    Returns *None* if the user cancels (Ctrl-C / EOF)
    or enters nothing.
    """
    try:
        password = getpass.getpass("Password: ")
    except EOFError, KeyboardInterrupt:
        print("\nOperation cancelled.", file=sys.stderr)
        return None

    if not password:
        print("Error: a password is required.", file=sys.stderr)
        return None

    return password


def read_optional_api_key(file: Path | None, env_var: str) -> str | None:
    """Read an API key from a file, falling back to an environment variable.

    Both sources are stripped: a key mounted from a Kubernetes secret
    or passed through ``--env-file`` usually carries a trailing newline.
    Returns ``None`` when neither source supplies a key,
    an unset variable and an empty one alike,
    which is how a service that accepts anonymous requests is reached.

    Argument validation only checks that *file* exists,
    so a path that exists but cannot be read arrives here.
    That is an expected, user-facing failure, not an unexpected one,
    so it is reported as an :class:`AppError`
    rather than left to escape as an ``OSError``.

    Raises:
        AppError: If *file* cannot be read.
    """
    if file is not None:
        try:
            return file.read_text(encoding="utf-8").strip()
        except OSError as e:
            # The OS message repeats the path, which is already named here.
            raise AppError(
                f"Could not read the API key from {file}: [Errno {e.errno}] {e.strerror}"
            ) from e
    return os.environ.get(env_var, "").strip() or None


def read_api_key(file: Path | None, env_var: str) -> str:
    """Read an API key as :func:`read_optional_api_key` does, but require one.

    Raises:
        AppError: If *file* cannot be read.
        DataShapeError: If neither source supplies a key.
    """
    value = read_optional_api_key(file, env_var)
    if value is None:
        raise DataShapeError(f"No API key available: {env_var} is unset or empty")
    return value
