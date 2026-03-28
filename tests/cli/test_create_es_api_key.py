"""Tests for :mod:`app.cli.create_es_api_key`."""

# This module exercises the command's formatting helpers directly.
# pyright: reportPrivateUsage=false

import argparse
import json
from typing import Any

import pytest

from app.cli import create_es_api_key
from app.cli.create_es_api_key import (
    _build_parser,
    _format_created_key_details,
    _format_created_key_json,
    _format_encoded_key,
    _run,
)
from app.core.errors import DataShapeError

_RESULT: dict[str, Any] = {
    "id": "abc123",
    "name": "my-api-key",
    "api_key": "raw-secret",
    "encoded": "ZW5jb2RlZA==",
    "expiration": 1_800_100_000_000,
}


class _StubClient:
    """Answers ``create_api_key`` with a prepared response."""

    def __init__(self, result: dict[str, Any]) -> None:
        """Store the response to return."""
        self.result = result
        self.request: tuple[str | None, str | None, str | None] | None = None

    def __enter__(self) -> "_StubClient":
        """Stand in for the client's context manager."""
        return self

    def __exit__(self, *exc_info: object) -> None:
        """Leave the context manager, suppressing nothing."""
        return None

    def create_api_key(
        self,
        *,
        key_name: str | None,
        expiration: str | None,
        username: str | None,
    ) -> dict[str, Any]:
        """Record the request and return the prepared response."""
        self.request = (key_name, expiration, username)
        return dict(self.result)


def _parse(*argv: str) -> argparse.Namespace:
    """Parse a minimal command line, plus whatever *argv* adds.

    ``--expiration`` is required, and argparse keeps the last occurrence,
    so a caller passing its own overrides the one supplied here.
    """
    return _build_parser().parse_args(
        ["--es-endpoint", "https://elastic.example.com:9200", "--expiration", "7d", *argv]
    )


def _output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    result: dict[str, Any],
    *argv: str,
) -> str:
    """Run the command against a stubbed client and return what it printed."""

    def _make_client(_args: argparse.Namespace) -> _StubClient:
        return _StubClient(result)

    monkeypatch.setattr(create_es_api_key, "make_es_client", _make_client)
    assert _run(_parse(*argv)) == 0
    return capsys.readouterr().out


def test_the_key_format_is_the_encoded_secret_alone() -> None:
    """Nothing else may reach stdout; the caller pipes this into a file."""
    assert _format_encoded_key(_RESULT) == "ZW5jb2RlZA=="


@pytest.mark.parametrize(
    "result",
    [{}, {"encoded": ""}, {"encoded": None}, {"encoded": 1234}],
    ids=["missing", "empty", "null", "not-a-string"],
)
def test_the_key_format_refuses_to_print_a_missing_secret(result: dict[str, Any]) -> None:
    """Exiting 0 with an empty secret would look like success."""
    with pytest.raises(DataShapeError, match="Elasticsearch returned no encoded API key"):
        _format_encoded_key(result)


def test_the_json_format_carries_the_expiration_when_there_is_one() -> None:
    """The four identifying fields are always present."""
    assert json.loads(_format_created_key_json(_RESULT)) == {
        "id": "abc123",
        "name": "my-api-key",
        "api_key": "raw-secret",
        "encoded": "ZW5jb2RlZA==",
        "expiration": 1_800_100_000_000,
    }


def test_the_json_format_omits_an_absent_expiration() -> None:
    """A key that never expires has no expiration to report."""
    result = {key: value for key, value in _RESULT.items() if key != "expiration"}
    assert "expiration" not in json.loads(_format_created_key_json(result))


def test_the_full_format_reports_every_field() -> None:
    """The whole block is the command's human-readable output."""
    assert _format_created_key_details(_RESULT) == "\n".join(
        [
            "Elasticsearch API Key Created Successfully:",
            "",
            "ID:       abc123",
            "Name:     my-api-key",
            "API Key:  raw-secret",
            "Encoded:  ZW5jb2RlZA==",
            "Expires:  1800100000000",
            "",
            "Use the 'Encoded' value for API authentication.",
        ]
    )


def test_the_full_format_says_never_without_an_expiration() -> None:
    """Absent means the key never expires, not that the field is unknown."""
    result = {key: value for key, value in _RESULT.items() if key != "expiration"}
    assert "Expires:  Never" in _format_created_key_details(result)


def test_the_full_format_marks_missing_fields() -> None:
    """A response missing everything still renders."""
    lines = _format_created_key_details({}).splitlines()
    assert lines[2:6] == [
        "ID:       N/A",
        "Name:     N/A",
        "API Key:  N/A",
        "Encoded:  N/A",
    ]


def test_the_expiration_is_required(capsys: pytest.CaptureFixture[str]) -> None:
    """A key that outlives everyone's memory of it must be asked for."""
    with pytest.raises(SystemExit) as exit_info:
        _build_parser().parse_args(["--es-endpoint", "https://elastic.example.com:9200"])
    assert exit_info.value.code == 2
    assert "--expiration" in capsys.readouterr().err


@pytest.mark.parametrize("spelling", ["never", "NEVER", "Never", " never "])
def test_never_asks_for_a_key_with_no_expiry(spelling: str) -> None:
    """None is what omits the field, which is how Elasticsearch is told."""
    assert _parse("--expiration", spelling).expiration is None


@pytest.mark.parametrize("value", ["7d", "30D", "12h", "90m", "500ms", "1nanos"])
def test_a_time_value_reaches_elasticsearch_as_written(value: str) -> None:
    """Elasticsearch is the authority on the units; this only screens typos."""
    assert _parse("--expiration", value).expiration == value


@pytest.mark.parametrize(
    "value",
    ["7 days", "1.5h", "d", "7", "", "0d", "7 d", "7dd"],
    ids=["prose", "fraction", "no-count", "no-unit", "empty", "zero", "spaced", "trailing"],
)
def test_a_malformed_expiration_is_refused_before_the_key_exists(
    value: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Learning of the typo from the server means a stray key to invalidate."""
    with pytest.raises(SystemExit) as exit_info:
        _parse("--expiration", value)
    assert exit_info.value.code == 2
    assert "--expiration" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("argument", "sent"),
    [("never", None), ("7d", "7d")],
    ids=["never", "duration"],
)
def test_the_expiration_reaches_the_client(
    argument: str,
    sent: str | None,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The sentinel is resolved by the parser, so the client never sees it."""
    client = _StubClient(_RESULT)

    def _make_client(_args: argparse.Namespace) -> _StubClient:
        return client

    monkeypatch.setattr(create_es_api_key, "make_es_client", _make_client)
    assert _run(_parse("--expiration", argument)) == 0
    capsys.readouterr()
    assert client.request == (None, sent, None)


def test_the_key_format_is_the_default(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Omitting --format prints the secret, ready to pipe."""
    assert _parse().format == "key"
    assert _output(monkeypatch, capsys, _RESULT) == "ZW5jb2RlZA==\n"


@pytest.mark.parametrize("name", ["key", "json", "full"])
def test_each_format_selects_its_own_formatter(
    name: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--format chooses among exactly these three."""
    formatters = {
        "key": _format_encoded_key,
        "json": _format_created_key_json,
        "full": _format_created_key_details,
    }
    printed = _output(monkeypatch, capsys, _RESULT, "--format", name)
    assert printed == formatters[name](_RESULT) + "\n"


def test_an_unknown_format_is_rejected_before_the_command_runs() -> None:
    """argparse enforces the choices, so the dispatch cannot miss."""
    with pytest.raises(SystemExit):
        _parse("--format", "yaml")


def test_the_format_help_names_what_the_key_format_prints() -> None:
    """The shared helper supplies the wording around this command's summary."""
    help_text = " ".join(_build_parser().format_help().split())
    expected = (
        "--format {key,json,full} Output format: 'key' (default, encoded key only), "
        + "'json', or 'full' (human-readable)"
    )
    assert expected in help_text
