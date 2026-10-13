"""Stand-ins for the Elasticsearch and Prometheus clients, for the tests.

A command opens its client as a context manager and calls one method on it,
so a stand-in only has to answer that call.
The commands are tested without connecting to anything,
which is why each one's ``make_es_client`` is replaced by :func:`install_client`
and ``make_prometheus_client`` by :func:`install_prometheus_client`.
"""

# A stub client answers one call, so it has just one public method.
# pylint: disable=too-few-public-methods

import argparse
import re
from types import ModuleType
from typing import Any, Self

import pytest

# The Pod a peak-memory query selects.
_POD_MATCHER = re.compile(r'pod="([^"]*)"')


class StubClient:
    """A client that can be entered and left, and does nothing else."""

    def __enter__(self) -> Self:
        """Stand in for the client's context manager."""
        return self

    def __exit__(self, *exc_info: object) -> None:
        """Leave the context manager, suppressing nothing."""
        return None


class QueryStubClient(StubClient):
    """Answers ``query`` with prepared documents, recording the request."""

    def __init__(self, documents: list[dict[str, Any]]) -> None:
        """Store the documents to yield."""
        self.documents = documents
        self.request: dict[str, Any] | None = None

    def query(self, **kwargs: Any) -> list[dict[str, Any]]:
        """Record the request and return the prepared documents."""
        self.request = kwargs
        return list(self.documents)


class PrometheusStubClient(StubClient):
    """Answers ``query`` with a prepared peak per Pod, recording each request.

    A Pod with no prepared peak gets an empty result vector,
    which is how Prometheus answers for a Pod it holds no samples for.
    """

    def __init__(self, peaks: dict[str, float]) -> None:
        """Store the peak, in bytes, to answer for each Pod name."""
        self.peaks = peaks
        self.requests: list[tuple[str, int]] = []

    def query(self, promql: str, time: int) -> list[dict[str, Any]]:
        """Record the request and answer for the Pod it selects."""
        self.requests.append((promql, time))
        match = _POD_MATCHER.search(promql)
        if match is None or match.group(1) not in self.peaks:
            return []
        return [{"metric": {}, "value": [time, str(self.peaks[match.group(1)])]}]


def install_client(
    monkeypatch: pytest.MonkeyPatch,
    command: ModuleType,
    client: StubClient,
) -> None:
    """Make *command* build *client* in place of its Elasticsearch client."""

    def _make_client(_args: argparse.Namespace) -> StubClient:
        return client

    monkeypatch.setattr(command, "make_es_client", _make_client)


def install_prometheus_client(
    monkeypatch: pytest.MonkeyPatch,
    command: ModuleType,
    client: PrometheusStubClient,
) -> None:
    """Make *command* build *client* in place of its Prometheus client."""

    def _make_client(_args: argparse.Namespace) -> PrometheusStubClient:
        return client

    monkeypatch.setattr(command, "make_prometheus_client", _make_client)
