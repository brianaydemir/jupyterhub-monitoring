"""Tests for :mod:`app.cli.runtime`.

Every command's ``main`` is one call to :func:`run_command`, so the exit
codes documented in the README are decided here.
"""

import argparse
import sys

import pytest

from app.cli.runtime import parse_and_validate, run_command
from app.core.errors import AppError, ExternalServiceError


def _build_parser() -> argparse.ArgumentParser:
    """Build a one-argument parser for the cases below."""
    parser = argparse.ArgumentParser(prog="cmd")
    parser.add_argument("--name")
    return parser


@pytest.fixture(autouse=True)
def fixed_argv(monkeypatch: pytest.MonkeyPatch) -> None:
    """Parse a fixed command line rather than pytest's own."""
    monkeypatch.setattr(sys, "argv", ["cmd", "--name", "value"])


def test_validators_see_the_parsed_arguments_and_the_parser() -> None:
    """A validator reports through the same parser that produced the args."""
    seen: list[tuple[argparse.Namespace, argparse.ArgumentParser]] = []

    args = parse_and_validate(_build_parser, [lambda a, p: seen.append((a, p))])

    assert args.name == "value"
    assert len(seen) == 1
    assert seen[0][0] is args


def test_validators_run_in_the_order_given() -> None:
    """A later validator may rely on an earlier one having passed."""
    order: list[int] = []
    parse_and_validate(
        _build_parser,
        [lambda a, p: order.append(1), lambda a, p: order.append(2)],
    )
    assert order == [1, 2]


def test_no_validators_is_allowed() -> None:
    """Commands without post-parse checks pass nothing."""
    assert parse_and_validate(_build_parser).name == "value"


def test_a_command_returns_its_runner_s_exit_code() -> None:
    """push-servers returns 1 on a partial push, so the value is passed on."""
    assert run_command(_build_parser, lambda args: 0) == 0
    assert run_command(_build_parser, lambda args: 1) == 1


def test_an_expected_failure_exits_one_with_its_message(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An AppError is the project's own, so it is reported without a trace."""

    def _run(args: argparse.Namespace) -> int:
        raise ExternalServiceError("Querying Elasticsearch failed: nope")

    assert run_command(_build_parser, _run) == 1
    assert capsys.readouterr().err == "Error: Querying Elasticsearch failed: nope\n"


def test_an_unexpected_failure_is_labelled_as_such(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Anything that is not an AppError still exits 1, but says it was a bug."""

    def _run(args: argparse.Namespace) -> int:
        raise RuntimeError("boom")

    assert run_command(_build_parser, _run) == 1
    assert capsys.readouterr().err == "Unexpected error: boom\n"


def test_a_failing_validator_stops_the_command(capsys: pytest.CaptureFixture[str]) -> None:
    """``parser.error`` raises SystemExit, which is left to reach the shell.

    That is what makes a usage error exit 2 rather than 1.
    """

    def _run(args: argparse.Namespace) -> int:
        raise AssertionError("the runner should not have been reached")

    with pytest.raises(SystemExit) as exit_info:
        run_command(_build_parser, _run, validators=[lambda a, p: p.error("no good")])

    assert exit_info.value.code == 2
    assert "no good" in capsys.readouterr().err


def test_an_error_raised_by_a_validator_is_reported_as_a_failure(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Validation runs inside the same guard as the command body."""

    def _validate(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
        raise AppError("bad input")

    assert run_command(_build_parser, lambda args: 0, validators=[_validate]) == 1
    assert capsys.readouterr().err == "Error: bad input\n"
