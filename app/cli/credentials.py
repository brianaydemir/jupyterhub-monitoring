"""Obtaining the credentials a command authenticates with."""

import getpass
import os
import sys
from pathlib import Path

from app.core.errors import AppError, DataShapeError


def prompt_credentials(
    username_arg: str | None,
) -> tuple[str, str] | None:
    """Prompt for username and password interactively.

    Returns *None* if the user cancels (Ctrl-C / EOF) or if either value is
    empty after entry.
    """
    if username_arg:
        username = username_arg
    else:
        try:
            username = input("Username: ")
        except EOFError, KeyboardInterrupt:
            print("\nOperation cancelled.", file=sys.stderr)
            return None

    try:
        password = getpass.getpass("Password: ")
    except EOFError, KeyboardInterrupt:
        print("\nOperation cancelled.", file=sys.stderr)
        return None

    if not username or not password:
        print("Error: Username and password are required.", file=sys.stderr)
        return None

    return username, password


def read_api_key(file: Path | None, env_var: str) -> str:
    """Read an API key from a file, falling back to an environment variable.

    Both sources are stripped: a key mounted from a Kubernetes secret or
    passed through ``--env-file`` usually carries a trailing newline.

    Argument validation only checks that *file* exists, so a path that
    exists but cannot be read arrives here. That is an expected,
    user-facing failure, not an unexpected one, so it is reported as an
    :class:`AppError` rather than left to escape as an ``OSError``.

    Raises:
        AppError: If *file* cannot be read.
        DataShapeError: If neither source supplies a key.
    """
    if file is not None:
        try:
            return file.read_text(encoding="utf-8").strip()
        except OSError as e:
            # The OS message repeats the path, which is already named here.
            raise AppError(
                f"Could not read the API key from {file}: [Errno {e.errno}] {e.strerror}"
            ) from e
    value = os.environ.get(env_var, "").strip()
    if not value:
        raise DataShapeError(f"No API key available: {env_var} is unset or empty")
    return value
