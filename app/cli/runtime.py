"""Shared parse/validate/run orchestration for CLI commands."""

import argparse
import sys
from collections.abc import Callable

from app.core.errors import AppError

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
