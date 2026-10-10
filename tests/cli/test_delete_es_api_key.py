"""Tests for :mod:`app.cli.delete_es_api_key`."""

# This module exercises the command's parser and internal helpers directly.
# pyright: reportPrivateUsage=false

# A stub client answers one call, so it has just one public method.
# pylint: disable=too-few-public-methods

import argparse
from typing import Any

import pytest

from app.cli import delete_es_api_key
from app.cli.delete_es_api_key import _build_parser, _error_detail, _run
from tests.helpers.stub_clients import StubClient, install_client


class _StubClient(StubClient):
    """Answers ``delete_api_key`` with a prepared response."""

    def __init__(self, result: dict[str, Any]) -> None:
        """Store the response to return."""
        self.result = result
        self.request: tuple[str | None, str | None] | None = None

    def delete_api_key(
        self,
        *,
        key_id: str | None = None,
        key_name: str | None = None,
    ) -> dict[str, Any]:
        """Record the request and return the prepared response."""
        self.request = (key_id, key_name)
        return dict(self.result)


def _parse(*argv: str) -> argparse.Namespace:
    """Parse a minimal command line, plus whatever *argv* adds."""
    return _build_parser().parse_args(
        ["--es-endpoint", "https://elastic.example.com:9200", *argv]
    )


def _run_with(
    monkeypatch: pytest.MonkeyPatch,
    result: dict[str, Any],
    *argv: str,
) -> tuple[int, _StubClient]:
    """Run the command against a stubbed client, returning its exit code."""
    client = _StubClient(result)
    install_client(monkeypatch, delete_es_api_key, client)
    return _run(_parse(*argv)), client


#
# The parser
#


@pytest.mark.parametrize(
    ("flag", "value", "expected"),
    [("--id", "abc123", ("abc123", None)), ("--name", "my-key", (None, "my-key"))],
)
def test_the_named_key_reaches_the_client(
    flag: str,
    value: str,
    expected: tuple[str | None, str | None],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Each flag maps onto its own client argument."""
    _, client = _run_with(monkeypatch, {"invalidated_api_keys": ["abc123"]}, flag, value)
    capsys.readouterr()
    assert client.request == expected


#
# Reporting the outcome
#


def test_invalidated_keys_are_listed(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A name may match more than one key, so the count is reported too."""
    code, _ = _run_with(
        monkeypatch,
        {"invalidated_api_keys": ["abc123", "def456"]},
        "--name",
        "my-key",
    )
    assert code == 0
    assert "Invalidated 2 API key(s): abc123, def456" in capsys.readouterr().out


def test_already_invalidated_keys_are_reported_separately(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Re-running the command is not a failure, but it is worth saying."""
    code, _ = _run_with(
        monkeypatch,
        {"previously_invalidated_api_keys": ["abc123"]},
        "--id",
        "abc123",
    )
    assert code == 0
    assert "Already invalidated: 1 key(s): abc123" in capsys.readouterr().out


def test_no_matching_keys_is_not_an_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Elasticsearch reports a miss as an empty result, not a failure."""
    code, _ = _run_with(monkeypatch, {}, "--name", "absent")
    assert code == 0
    assert "No matching API keys found." in capsys.readouterr().out


def test_reported_errors_exit_one_with_their_details(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A partial failure must not look like success to a caller."""
    code, _ = _run_with(
        monkeypatch,
        {
            "invalidated_api_keys": ["abc123"],
            "error_count": 1,
            "error_details": [{"type": "security_exception", "reason": "not permitted"}],
        },
        "--name",
        "my-key",
    )
    captured = capsys.readouterr()
    assert code == 1
    assert "Invalidated 1 API key(s): abc123" in captured.out
    assert "Errors: 1" in captured.err
    assert "security_exception: not permitted" in captured.err


#
# _error_detail
#


def test_an_error_detail_names_its_type_and_reason() -> None:
    """Those two fields are what identify the failure."""
    detail = {"type": "security_exception", "reason": "not permitted", "extra": "ignored"}
    assert _error_detail(detail) == "security_exception: not permitted"


def test_an_error_detail_falls_back_when_fields_are_missing() -> None:
    """A response shape that is not the documented one still renders."""
    assert _error_detail({}) == "error: no reason given"


def test_a_non_mapping_error_detail_is_shown_as_it_arrived() -> None:
    """Anything unrecognized is printed rather than dropped."""
    assert _error_detail("something went wrong") == "something went wrong"
