"""Obtaining the credentials a command authenticates with."""

import getpass
import os
import sys
from pathlib import Path

from app.core.errors import AppError, DataShapeError


def prompt_password() -> str | None:
    """Prompt for a password interactively.

    Returns:
        The password, or *None* if the user cancels (Ctrl-C / EOF)
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

    Argument validation only checks that *file* exists,
    so a path that exists but cannot be read, or holds nothing, arrives here.
    That is an expected, user-facing failure, not an unexpected one,
    so it is reported as an :class:`AppError`
    rather than left to escape as an ``OSError``
    or sent on as an empty credential.

    Returns:
        The key, or ``None`` when no *file* is given
        and the variable is unset or empty,
        which is how a service that accepts anonymous requests is reached.

    Raises:
        AppError: If *file* cannot be read or holds no key.
    """
    if file is not None:
        try:
            key = file.read_text(encoding="utf-8").strip()
        except OSError as e:
            # The OS message repeats the path, which is already named here.
            raise AppError(
                f"Could not read the API key from {file}: [Errno {e.errno}] {e.strerror}"
            ) from e
        if not key:
            raise AppError(f"Could not read the API key from {file}: the file is empty")
        return key
    return os.environ.get(env_var, "").strip() or None


def read_api_key(file: Path | None, env_var: str) -> str:
    """Read an API key as :func:`read_optional_api_key` does, but require one.

    Raises:
        AppError: If *file* cannot be read or holds no key.
        DataShapeError: If neither source supplies a key.
    """
    value = read_optional_api_key(file, env_var)
    if value is None:
        raise DataShapeError(f"No API key available: {env_var} is unset or empty")
    return value
