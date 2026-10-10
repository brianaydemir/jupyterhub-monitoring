"""Tests for :mod:`app.usage.snapshots`."""

from datetime import datetime, timezone
from typing import Any

import pytest

from app.usage.snapshots import build_query, select_hub, snapshot_seconds, started_seconds


def test_query_window_is_half_open() -> None:
    """The end is excluded so back-to-back reports do not overlap."""
    window = _range_filter(build_query(cutoff=100, end=200))
    assert window == {"gte": 100, "lt": 200}


def test_query_matches_ready_and_spawning_servers() -> None:
    """A spawning server is active time too, so both states count."""
    clauses = _filter_clauses(build_query(cutoff=0, end=1))
    states = [c for c in clauses if "bool" in c][0]["bool"]["should"]
    assert {"term": {"server.ready": True}} in states
    assert {"term": {"server.pending": "spawn"}} in states


def test_query_excludes_testing_documents() -> None:
    """Documents tagged as testing never count toward a report."""
    excluded = build_query(cutoff=0, end=1)["bool"]["must_not"][0]["bool"]["should"]
    assert {"term": {"meta.testing": "true"}} in excluded
    assert {"term": {"meta.testing": True}} in excluded


def test_query_filters_by_hub_when_asked() -> None:
    """Both the text and keyword mappings of ``meta.hub`` are matched."""
    clauses = _filter_clauses(build_query(cutoff=0, end=1, hub="production"))
    hub_clauses = [
        c
        for c in clauses
        if "bool" in c and any("meta.hub" in str(s) for s in c["bool"]["should"])
    ]
    assert len(hub_clauses) == 1
    assert {"term": {"meta.hub": "production"}} in hub_clauses[0]["bool"]["should"]
    assert {"term": {"meta.hub.keyword": "production"}} in hub_clauses[0]["bool"]["should"]


def test_query_omits_the_hub_filter_by_default() -> None:
    """Without ``--hub`` the query spans every hub in the index."""
    assert "meta.hub" not in str(build_query(cutoff=0, end=1))


def test_select_hub_keeps_only_an_exact_match() -> None:
    """A text mapping's term query also returns hubs sharing a word."""
    docs: list[dict[str, Any]] = [
        {"meta.hub": "osg"},
        {"meta.hub": "osg-dev"},
        {"meta.hub": "OSG"},
        {},
    ]
    assert list(select_hub(docs, "osg")) == [{"meta.hub": "osg"}]


def test_select_hub_matches_a_hub_that_is_not_a_string_by_its_text() -> None:
    """A mapping need not store the hub as text, but --hub is always text."""
    assert list(select_hub([{"meta.hub": 7}], "7")) == [{"meta.hub": 7}]


def test_select_hub_keeps_everything_without_a_hub() -> None:
    """Without --hub the report spans every hub in the index."""
    docs: list[dict[str, Any]] = [{"meta.hub": "a"}, {"meta.hub": "b"}, {}]
    assert list(select_hub(docs, None)) == docs


@pytest.mark.parametrize(
    ("value", "expected"),
    [(1782000000, 1782000000), (1782000000.7, 1782000000), (0, 0)],
)
def test_snapshot_seconds_accepts_numbers(value: object, expected: int) -> None:
    """Snapshot times are epoch seconds, stored as an int or a float."""
    assert snapshot_seconds({"meta.snapshot-time": value}) == expected


@pytest.mark.parametrize("value", [None, True, False, "1782000000", [], {}])
def test_snapshot_seconds_rejects_non_numbers(value: object) -> None:
    """A bool is not a timestamp, though ``isinstance`` would admit it."""
    assert snapshot_seconds({"meta.snapshot-time": value}) is None


def test_started_seconds_reads_an_iso_timestamp() -> None:
    """A recorded start is an ISO 8601 string, reported as epoch seconds."""
    expected = int(datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc).timestamp())
    assert started_seconds({"server.started": "2026-07-02T09:00:00Z"}) == expected


@pytest.mark.parametrize("doc", [{}, {"server.started": None}, {"server.started": "whenever"}])
def test_started_seconds_rejects_a_missing_or_unparsable_value(doc: dict[str, Any]) -> None:
    """A snapshot taken while spawning has no start time to read."""
    assert started_seconds(doc) is None


def _filter_clauses(query: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the query's ``filter`` clauses."""
    return list(query["bool"]["filter"])


def _range_filter(query: dict[str, Any]) -> dict[str, Any]:
    """Return the snapshot-time range bounds from the query."""
    for clause in _filter_clauses(query):
        if "range" in clause:
            return dict(clause["range"]["meta.snapshot-time"])
    raise AssertionError("no range clause in query")
