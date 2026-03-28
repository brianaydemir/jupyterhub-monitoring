"""Tests for :mod:`app.cli.get_es_docs`."""

# This module exercises the command's parser, validator, and runner directly.
# pyright: reportPrivateUsage=false

import argparse
import json
from typing import Any

import pytest

from app.cli import get_es_docs
from app.cli.get_es_docs import _build_parser, _run, _validate_arguments


class _StubClient:
    """Answers ``query`` with prepared documents, recording the request."""

    def __init__(self, documents: list[dict[str, Any]]) -> None:
        """Store the documents to yield."""
        self.documents = documents
        self.request: dict[str, Any] | None = None

    def __enter__(self) -> "_StubClient":
        """Stand in for the client's context manager."""
        return self

    def __exit__(self, *exc_info: object) -> None:
        """Leave the context manager, suppressing nothing."""
        return None

    def query(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Record the request and return the prepared documents."""
        self.request = kwargs
        return list(self.documents)


def _parse(*argv: str) -> argparse.Namespace:
    """Parse a minimal command line, plus whatever *argv* adds."""
    return _build_parser().parse_args(
        [
            "--es-endpoint",
            "https://elastic.example.com:9200",
            "--es-index",
            "servers",
            "--query",
            "meta.hub:production",
            *argv,
        ]
    )


def _run_with(
    monkeypatch: pytest.MonkeyPatch,
    documents: list[dict[str, Any]],
    *argv: str,
) -> tuple[int, _StubClient]:
    """Run the command against a stubbed client, returning its exit code."""
    client = _StubClient(documents)

    def _make_client(_args: argparse.Namespace) -> _StubClient:
        return client

    monkeypatch.setattr(get_es_docs, "make_es_client", _make_client)
    return _run(_parse(*argv)), client


def test_the_query_is_required(capsys: pytest.CaptureFixture[str]) -> None:
    """The command exists to run one, so there is no default."""
    with pytest.raises(SystemExit) as exit_info:
        _build_parser().parse_args(
            ["--es-endpoint", "https://elastic.example.com:9200", "--es-index", "servers"]
        )
    assert exit_info.value.code == 2
    assert "--query" in capsys.readouterr().err


def test_a_non_positive_limit_is_rejected(capsys: pytest.CaptureFixture[str]) -> None:
    """A cap of zero would retrieve nothing, which is surely a mistake."""
    parser = _build_parser()
    args = parser.parse_args(
        [
            "--es-endpoint",
            "https://elastic.example.com:9200",
            "--es-index",
            "servers",
            "--es-username",
            "elastic",
            "--query",
            "*",
            "--limit",
            "0",
        ]
    )
    with pytest.raises(SystemExit) as exit_info:
        _validate_arguments(args, parser)
    assert exit_info.value.code == 2
    assert "--limit must be a positive integer" in capsys.readouterr().err


def test_documents_are_printed_as_pretty_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The output is a stream of JSON objects, meant for reading."""
    documents = [{"user": {"name": "a@x.edu"}}, {"user": {"name": "b@x.edu"}}]
    code, _ = _run_with(monkeypatch, documents)
    printed = capsys.readouterr().out
    assert code == 0
    assert printed == "".join(json.dumps(doc, indent=2) + "\n" for doc in documents)


def test_an_unlimited_query_is_left_unsorted(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without a cap, the index's own order costs nothing to accept."""
    _, client = _run_with(monkeypatch, [])
    assert client.request is not None
    assert client.request["sort"] is None
    assert client.request["limit"] is None
    assert client.request["query_string"] == "meta.hub:production"
    assert client.request["index"] == "servers"


def test_a_limited_query_keeps_the_newest_documents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sorting is tied to --limit, so the cap keeps the newest snapshots."""
    _, client = _run_with(monkeypatch, [], "--limit", "5")
    assert client.request is not None
    assert client.request["sort"] == [{"meta.snapshot-time": {"order": "desc"}}]
    assert client.request["limit"] == 5
