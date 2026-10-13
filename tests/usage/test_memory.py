"""Tests for :mod:`app.usage.memory`."""

from datetime import datetime, timezone
from typing import Any

import pytest

from app.usage.memory import MemoryTally, measure_peak_memory
from tests.helpers.stub_clients import PrometheusStubClient

_GIB = 2**30


def _doc(
    user: str = "a@x.edu",
    *,
    started: str | None = "2026-08-05T10:00:00Z",
    pod: str | None = "jupyter-a",
    snapshot: str = "2026-08-05T10:02:00Z",
) -> dict[str, Any]:
    """Build a countable snapshot of *user*'s default server, taken at *snapshot*."""
    doc: dict[str, Any] = {
        "user.name": user,
        "server.name": "",
        "meta.interval": "5m",
        "meta.snapshot-time": _epoch(snapshot),
    }
    if started is not None:
        doc["server.started"] = started
    if pod is not None:
        doc["server.state.pod_name"] = pod
        doc["server.state.namespace"] = "jhub"
    return doc


def _epoch(iso: str) -> int:
    """Return an ISO 8601 UTC timestamp as epoch seconds."""
    return int(datetime.fromisoformat(iso).astimezone(timezone.utc).timestamp())


@pytest.mark.parametrize(
    ("started", "seconds"),
    [
        # The first snapshot's interval reaches back past the start,
        # where an earlier run of the same-named Pod may have been.
        ("2026-08-05T10:00:00Z", 720),
        # The server started long before: the span is the time the report
        # credits, from one interval before the first snapshot.
        ("2026-08-05T08:00:00Z", 900),
    ],
    ids=["clipped-to-start", "one-interval-before-first-snapshot"],
)
def test_a_session_is_measured_over_the_time_its_snapshots_cover(
    started: str, seconds: int
) -> None:
    """The query selects the session's Pod and the stretch the report credits.

    KubeSpawner reuses a server's Pod name on every start,
    so a span reaching back past this session's start
    could report the previous session's peak as this one's.
    """
    client = PrometheusStubClient({"jupyter-a": 2 * _GIB})
    documents = [
        _doc(started=started, snapshot="2026-08-05T10:02:00Z"),
        _doc(started=started, snapshot="2026-08-05T10:07:00Z"),
        _doc(started=started, snapshot="2026-08-05T10:12:00Z"),
    ]

    peaks, _ = measure_peak_memory(documents, client)

    promql = (
        "max_over_time(sum(container_memory_working_set_bytes"
        '{namespace="jhub",pod="jupyter-a",container!="",image!=""})'
        f"[{seconds}s:])"
    )
    assert client.requests == [(promql, _epoch("2026-08-05T10:12:00Z"))]
    assert peaks == {("a@x.edu", "", started): 2.0 * _GIB}


def test_sessions_that_cannot_be_measured_are_tallied_by_reason() -> None:
    """A session naming no Pod and one without samples have different fixes.

    The first points at the scopes of ``push-servers``' token,
    and the second at what Prometheus retained;
    a session still spawning is neither, and is not queried at all.
    """
    client = PrometheusStubClient({"jupyter-d": 1 * _GIB})
    documents = [
        _doc("a@x.edu", started=None, pod="jupyter-a"),
        _doc("b@x.edu", pod=None),
        _doc("c@x.edu", pod="jupyter-c"),
        _doc("d@x.edu", pod="jupyter-d"),
    ]

    peaks, tally = measure_peak_memory(documents, client)

    assert list(peaks) == [("d@x.edu", "", "2026-08-05T10:00:00Z")]
    assert tally == MemoryTally(started=3, unnamed=1, measured=1)
    assert tally.no_samples == 1
    assert [promql for promql, _ in client.requests if "jupyter-a" in promql] == []
