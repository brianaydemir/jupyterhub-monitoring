"""Tests for :mod:`app.cli.list_es_api_keys`."""

# This module exercises the command's formatting helpers directly.
# pyright: reportPrivateUsage=false

# A stub client answers one call, so it has just one public method.
# pylint: disable=too-few-public-methods

import argparse
import json
from typing import Any

import pytest

from app.cli import list_es_api_keys
from app.cli.list_es_api_keys import (
    _build_parser,
    _format_key_ids,
    _format_key_list_details,
    _format_key_list_json,
    _run,
)
from tests.helpers.stub_clients import StubClient, install_client

_NOW_MS = 1_800_000_000_000
_CREATED_MS = 1_799_000_000_000  # 2027-01-03 18:13:20 UTC
_PAST_MS = 1_750_000_000_000  # 2025-06-15 15:06:40 UTC
_FUTURE_MS = 1_900_000_000_000  # 2030-03-17 17:46:40 UTC

_ACTIVE: dict[str, Any] = {
    "id": "key-1",
    "name": "active-key",
    "creation": _CREATED_MS,
    "expiration": _FUTURE_MS,
    "invalidated": False,
}
_EXPIRED: dict[str, Any] = {
    "id": "key-2",
    "name": "expired-key",
    "creation": _CREATED_MS,
    "expiration": _PAST_MS,
    "invalidated": False,
}
_INVALIDATED: dict[str, Any] = {
    "id": "key-3",
    "name": "invalidated-key",
    "creation": _CREATED_MS,
    "expiration": _FUTURE_MS,
    "invalidated": True,
}
_GONE: dict[str, Any] = {
    "id": "key-4",
    "name": "gone-key",
    "creation": _CREATED_MS,
    "expiration": _PAST_MS,
    "invalidated": True,
}
_ETERNAL: dict[str, Any] = {
    "id": "key-5",
    "name": "eternal-key",
    "creation": _CREATED_MS,
    "invalidated": False,
}
_UNDATED: dict[str, Any] = {
    "id": "key-6",
    "name": "undated-key",
    "expiration": _FUTURE_MS,
    "invalidated": False,
}


class _StubClient(StubClient):
    """Answers ``list_api_keys`` with a prepared list."""

    def __init__(self, keys: list[dict[str, Any]]) -> None:
        """Store the keys to return."""
        self.keys = keys
        self.active_only: bool | None = None

    def list_api_keys(self, *, active_only: bool = True) -> list[dict[str, Any]]:
        """Record the request and return the prepared keys."""
        self.active_only = active_only
        return list(self.keys)


def _freeze_now(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin "now", which decides which keys count as expired."""
    monkeypatch.setattr(list_es_api_keys, "get_now_ms", lambda: _NOW_MS)


def _parse(*argv: str) -> argparse.Namespace:
    """Parse a minimal command line, plus whatever *argv* adds."""
    return _build_parser().parse_args(
        ["--es-endpoint", "https://elastic.example.com:9200", *argv]
    )


def _install(
    monkeypatch: pytest.MonkeyPatch,
    keys: list[dict[str, Any]],
) -> _StubClient:
    """Put a stubbed client behind the command and return it."""
    client = _StubClient(keys)
    _freeze_now(monkeypatch)
    install_client(monkeypatch, list_es_api_keys, client)
    return client


def _output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    keys: list[dict[str, Any]],
    *argv: str,
) -> str:
    """Run the command against a stubbed client and return what it printed."""
    _install(monkeypatch, keys)
    assert _run(_parse(*argv)) == 0
    return capsys.readouterr().out


def test_the_key_format_is_one_id_and_name_per_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """This is the form other tools parse, so it stays column-simple."""
    _freeze_now(monkeypatch)
    assert _format_key_ids([_ACTIVE, _ETERNAL]) == "key-1  active-key\nkey-5  eternal-key"


def test_the_key_format_tags_a_key_that_is_no_longer_usable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both reasons are reported, invalidation first."""
    _freeze_now(monkeypatch)
    assert _format_key_ids([_EXPIRED, _INVALIDATED, _GONE], active_only=False) == "\n".join(
        [
            "key-2  expired-key  [expired]",
            "key-3  invalidated-key  [invalidated]",
            "key-4  gone-key  [invalidated, expired]",
        ]
    )


def test_a_key_expiring_exactly_now_is_expired(monkeypatch: pytest.MonkeyPatch) -> None:
    """The comparison is inclusive, so the boundary is not usable."""
    _freeze_now(monkeypatch)
    key = {"id": "key-7", "name": "boundary-key", "expiration": _NOW_MS}
    assert _format_key_ids([key]) == "key-7  boundary-key  [expired]"


def test_the_key_format_says_which_listing_came_up_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The wording distinguishes "none active" from "none at all"."""
    _freeze_now(monkeypatch)
    assert _format_key_ids([]) == "(no active API keys)"
    assert _format_key_ids([], active_only=False) == "(no API keys)"


def test_the_json_format_is_the_response_verbatim() -> None:
    """Nothing is added or dropped; the caller reads the raw fields."""
    keys = [_ACTIVE, _EXPIRED]
    assert _format_key_list_json(keys) == json.dumps(keys, indent=2)
    assert json.loads(_format_key_list_json(keys)) == keys


def test_the_full_format_describes_an_active_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """The default listing counts the keys and omits the invalidated flag."""
    _freeze_now(monkeypatch)
    assert _format_key_list_details([_ACTIVE]) == "\n".join(
        [
            "Active API Keys (1):",
            "",
            "  ID:          key-1",
            "  Name:        active-key",
            "  Created:     2027-01-03 18:13:20 UTC",
            "  Expires:     2030-03-17 17:46:40 UTC",
        ]
    )


def test_the_full_format_shows_invalidation_only_with_all(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--all is what makes the flag worth a line."""
    _freeze_now(monkeypatch)
    assert _format_key_list_details([_INVALIDATED], active_only=False) == "\n".join(
        [
            "API Keys (1):",
            "",
            "  ID:          key-3",
            "  Name:        invalidated-key",
            "  Created:     2027-01-03 18:13:20 UTC",
            "  Expires:     2030-03-17 17:46:40 UTC",
            "  Invalidated: True",
        ]
    )


def test_the_full_format_marks_an_expiration_that_has_passed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The date alone does not say whether it is in the past."""
    _freeze_now(monkeypatch)
    assert "  Expires:     2025-06-15 15:06:40 UTC  (expired)" in _format_key_list_details(
        [_EXPIRED]
    )


def test_the_full_format_distinguishes_never_from_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An absent expiry means never; an absent creation is just missing."""
    _freeze_now(monkeypatch)
    assert "  Expires:     Never" in _format_key_list_details([_ETERNAL])
    assert "  Created:     unknown" in _format_key_list_details([_UNDATED])


def test_the_full_format_separates_keys_with_a_blank_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The trailing separator is trimmed, so the output ends on a field."""
    _freeze_now(monkeypatch)
    rendered = _format_key_list_details([_ACTIVE, _ETERNAL])
    assert "  Expires:     2030-03-17 17:46:40 UTC\n\n  ID:          key-5" in rendered
    assert not rendered.endswith("\n")


def test_the_full_format_says_which_listing_came_up_empty() -> None:
    """The empty wording follows the same distinction as the key format."""
    assert _format_key_list_details([]) == "No active API keys found."
    assert _format_key_list_details([], active_only=False) == "No API keys found."


@pytest.mark.parametrize("name", ["key", "json", "full"])
def test_each_format_selects_its_own_formatter(
    name: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--format chooses among exactly these three."""
    keys = [_ACTIVE, _ETERNAL]
    expected = {
        "key": _format_key_ids(keys),
        "json": _format_key_list_json(keys),
        "full": _format_key_list_details(keys),
    }
    printed = _output(monkeypatch, capsys, keys, "--format", name)
    assert printed == expected[name] + "\n"


def test_the_default_listing_asks_only_for_active_keys(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The narrower query and the narrower wording go together."""
    client = _install(monkeypatch, [])
    assert _run(_parse()) == 0
    assert client.active_only is True
    assert capsys.readouterr().out == "(no active API keys)\n"


@pytest.mark.parametrize(
    ("name", "expected"),
    [("key", "(no API keys)\n"), ("full", "No API keys found.\n")],
)
def test_all_widens_both_the_query_and_the_wording(
    name: str,
    expected: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--all asks the client for every key, and the empty listing says so."""
    client = _install(monkeypatch, [])
    assert _run(_parse("--all", "--format", name)) == 0
    assert client.active_only is False
    assert capsys.readouterr().out == expected
