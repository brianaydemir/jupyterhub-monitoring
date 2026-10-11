"""Tests for :mod:`app.cli.runtime`.

Every command's ``main`` is one call to :func:`run_command`,
so the exit codes documented in the README are decided here.
"""

import argparse
import sys
from collections.abc import Callable

import pytest

from app.cli import (
    create_es_api_key,
    delete_es_api_key,
    get_es_docs,
    get_new_activity,
    get_new_users,
    list_es_api_keys,
    push_servers,
)
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
    """A validator is handed the parsed arguments and a parser to report through."""
    seen: list[tuple[argparse.Namespace, argparse.ArgumentParser]] = []

    args = parse_and_validate(_build_parser, [lambda a, p: seen.append((a, p))])

    assert args.name == "value"
    assert len(seen) == 1
    assert seen[0][0] is args
    assert seen[0][1].prog == "cmd"


def test_validators_run_in_the_order_given() -> None:
    """A later validator may rely on an earlier one having passed."""
    order: list[int] = []
    parse_and_validate(
        _build_parser,
        [lambda a, p: order.append(1), lambda a, p: order.append(2)],
    )
    assert order == [1, 2]


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


_HUB = ["--jupyterhub-endpoint", "https://hub.example.edu/hub/api"]
_ES = ["--es-endpoint", "https://es.example.edu:9200"]
_CREDENTIALS_REQUIRED = "one of --es-api-key, ELASTICSEARCH_API_KEY, or --es-username"


@pytest.mark.parametrize(
    ("main", "argv", "message"),
    [
        (push_servers.main, [*_HUB, "--debug", "--interval", "0s"], "must be positive"),
        (get_new_activity.main, [*_ES, "--es-index", "i"], _CREDENTIALS_REQUIRED),
        (get_new_users.main, [*_HUB, "--duration", "1 day", "--hub", "h"], "--hub requires"),
        (get_es_docs.main, [*_ES, "--es-index", "i", "--query", "q"], _CREDENTIALS_REQUIRED),
        (create_es_api_key.main, [*_ES, "--expiration", "7d"], _CREDENTIALS_REQUIRED),
        (delete_es_api_key.main, [*_ES, "--id", "k"], _CREDENTIALS_REQUIRED),
        (list_es_api_keys.main, _ES, _CREDENTIALS_REQUIRED),
    ],
    ids=[
        "push-servers",
        "get-new-activity",
        "get-new-users",
        "get-es-docs",
        "create-es-api-key",
        "delete-es-api-key",
        "list-es-api-keys",
    ],
)
def test_every_command_runs_its_own_validators(
    main: Callable[[], int],
    argv: list[str],
    message: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A command line argparse accepts is still a usage error, exiting 2."""
    monkeypatch.setenv("JUPYTERHUB_API_KEY", "secret")
    monkeypatch.delenv("ELASTICSEARCH_API_KEY", raising=False)
    monkeypatch.setattr(sys, "argv", ["cmd", *argv])
    with pytest.raises(SystemExit) as exit_info:
        main()
    assert exit_info.value.code == 2
    assert message in capsys.readouterr().err
