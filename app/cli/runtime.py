"""Shared runtime helpers for CLI command execution."""

import argparse
import sys
from collections.abc import Callable
from datetime import timedelta

from app.core.errors import AppError, DataShapeError
from app.core.time_utils import parse_duration

Builder = Callable[[], argparse.ArgumentParser]
Validator = Callable[[argparse.Namespace, argparse.ArgumentParser], None]
Runner = Callable[[argparse.Namespace], int]


def parse_and_validate(
    parser_builder: Builder,
    validators: list[Validator] | None = None,
) -> argparse.Namespace:
    """Build a parser, parse CLI args, and run post-parse validators."""
    parser = parser_builder()
    args = parser.parse_args()
    for validator in validators or []:
        validator(args, parser)
    return args


def run_command(
    parser_builder: Builder,
    run: Runner,
    validators: list[Validator] | None = None,
) -> int:
    """Execute a command with shared parse/validate/error handling."""
    try:
        args = parse_and_validate(parser_builder, validators)
        return run(args)
    except AppError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


def positive_int_or_error(
    value: int | None,
    *,
    parser: argparse.ArgumentParser,
    flag: str,
) -> None:
    """Validate that an optional integer is positive when provided."""
    if value is not None and value < 1:
        parser.error(f"{flag} must be a positive integer")


def parse_duration_required(duration: str) -> timedelta:
    """Parse a required duration string.

    Raises:
        DataShapeError: If *duration* cannot be parsed.
    """
    parsed = parse_duration(duration)
    if parsed is None:
        raise DataShapeError(f"Invalid duration format: {duration!r}")
    return parsed
