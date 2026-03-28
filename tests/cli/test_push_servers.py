"""Tests for :mod:`app.cli.push_servers`.

The pipeline itself lives in :mod:`app.usage.push` and is covered by
``tests/usage/test_push.py``. What is left here is the command's own
post-parse validation, which decides what ``--debug`` excuses.
"""

# This module exercises the command's parser and validator directly.
# pyright: reportPrivateUsage=false

import argparse

import pytest

from app.cli.push_servers import _build_parser, _validate_arguments

_HUB = ["--jupyterhub-endpoint", "https://hub.example.edu/hub/api"]
_ES = ["--es-endpoint", "https://es.example.edu:9200", "--es-index", "servers"]


@pytest.fixture(autouse=True)
def supply_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Supply both API keys, so only the checks under test can fail."""
    monkeypatch.setenv("JUPYTERHUB_API_KEY", "secret")
    monkeypatch.setenv("ELASTICSEARCH_API_KEY", "secret")


def _validate(argv: list[str]) -> argparse.ArgumentParser:
    """Parse and validate *argv*, returning the parser that was used."""
    parser = _build_parser()
    _validate_arguments(parser.parse_args(argv), parser)
    return parser


def _reject(argv: list[str], capsys: pytest.CaptureFixture[str]) -> str:
    """Validate *argv*, assert it exited 2, and return what it printed."""
    with pytest.raises(SystemExit) as exit_info:
        _validate(argv)
    assert exit_info.value.code == 2
    return capsys.readouterr().err


def test_a_complete_push_is_accepted() -> None:
    """The documented invocation passes every check."""
    _validate(_HUB + _ES + ["--interval", "5m", "--metadata", "hub=production"])


def test_the_es_endpoint_is_required_without_debug(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The group is declared optional, so the requirement is checked here."""
    message = _reject(_HUB + ["--interval", "5m"], capsys)
    assert "--es-endpoint is required unless --debug is used" in message


def test_the_es_index_is_required_without_debug(capsys: pytest.CaptureFixture[str]) -> None:
    """A connection with no index has nowhere to push to."""
    argv = _HUB + ["--es-endpoint", "https://es.example.edu:9200", "--interval", "5m"]
    assert "--es-index is required unless --debug is used" in _reject(argv, capsys)


def test_an_es_credential_is_required_without_debug(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Once Elasticsearch is in use, the shared credential rules apply."""
    monkeypatch.delenv("ELASTICSEARCH_API_KEY")
    message = _reject(_HUB + _ES + ["--interval", "5m"], capsys)
    assert "one of --es-api-key, ELASTICSEARCH_API_KEY, or --es-username is required" in message


@pytest.mark.parametrize("mode", [_ES, ["--debug"]])
def test_the_interval_is_always_required(
    mode: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """get-new-activity adds meta.interval up, so a push must record it."""
    message = _reject(_HUB + mode, capsys)
    assert "the following arguments are required: --interval" in message


def test_debug_needs_no_elasticsearch_but_needs_an_interval(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Debug mode prints documents, so it has nothing to connect to.

    It still stamps meta.interval, so the interval is not excused.
    """
    _validate(_HUB + ["--debug", "--interval", "5m"])
    assert capsys.readouterr().err == ""


def test_an_unparsable_interval_is_rejected(capsys: pytest.CaptureFixture[str]) -> None:
    """The interval is a duration, and is checked even in debug mode."""
    message = _reject(_HUB + ["--debug", "--interval", "5 fortnights"], capsys)
    assert "--interval: invalid duration '5 fortnights'" in message


@pytest.mark.parametrize("interval", ["0s", "-5m"])
def test_a_non_positive_interval_is_rejected(
    interval: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A zero or backwards cadence would make uptime meaningless."""
    message = _reject(_HUB + _ES + ["--interval", interval], capsys)
    assert f"--interval must be positive, not {interval!r}" in message


def test_a_non_positive_limit_is_rejected(capsys: pytest.CaptureFixture[str]) -> None:
    """--limit caps how many servers are processed, so zero is a mistake."""
    argv = _HUB + _ES + ["--interval", "5m", "--limit", "0"]
    assert "--limit must be a positive integer" in _reject(argv, capsys)


def test_malformed_metadata_is_reported_as_a_usage_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A typo in --metadata is the user's mistake, not a runtime failure."""
    argv = _HUB + _ES + ["--interval", "5m", "--metadata", "hub"]
    assert "hub" in _reject(argv, capsys)


def test_reserved_metadata_keys_are_reported_as_a_usage_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The command sets these itself, so a caller cannot also supply them."""
    argv = _HUB + _ES + ["--interval", "5m", "--metadata", "interval=5m"]
    assert "interval" in _reject(argv, capsys)
