"""Tests for :mod:`app.cli.get_es_docs`."""

# This module exercises the command's parser, validator, and runner directly.
# pyright: reportPrivateUsage=false

import argparse
import json
from typing import Any

import pytest

from app.cli import get_es_docs
from app.cli.get_es_docs import _build_parser, _run, _validate_arguments
from tests.helpers.stub_clients import QueryStubClient, install_client


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
) -> tuple[int, QueryStubClient]:
    """Run the command against a stubbed client, returning its exit code."""
    client = QueryStubClient(documents)
    install_client(monkeypatch, get_es_docs, client)
    return _run(_parse(*argv)), client


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
    """Sorting is tied to --limit, so the cap keeps the newest snapshots.

    The sort tolerates an index that has no ``meta.snapshot-time`` mapping,
    which Elasticsearch would otherwise reject.
    """
    _, client = _run_with(monkeypatch, [], "--limit", "5")
    assert client.request is not None
    assert client.request["sort"] == [
        {"meta.snapshot-time": {"order": "desc", "unmapped_type": "long"}}
    ]
    assert client.request["limit"] == 5
