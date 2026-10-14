"""Peak memory per session, read from Prometheus.

A session runs in a KubeSpawner Pod, which the spawner names in its state.
``push-servers`` records that state as ``server.state.pod_name``
and ``server.state.namespace``
when its JupyterHub token holds the ``admin:server_state`` scope.

The peak is taken from ``container_memory_working_set_bytes``,
the figure the kubelet holds a container's memory limit against,
summed over the Pod's containers.
"""

import json
from collections.abc import Iterable
from dataclasses import dataclass, replace
from typing import Any, Protocol

from app.usage.activity import SessionKey, session_contributions
from app.usage.snapshots import snapshot_seconds, started_seconds


# pylint: disable=too-few-public-methods
class MetricsQuerier(Protocol):
    """Evaluates PromQL instant queries."""

    def query(self, promql: str, time: int) -> list[dict[str, Any]]:
        """Return the result vector of *promql* evaluated at *time*.

        Raises:
            ExternalServiceError: If the query fails.
        """
        raise NotImplementedError


# pylint: enable=too-few-public-methods


@dataclass(frozen=True)
class MemoryTally:
    """How many started sessions a run measured, and why it missed the rest.

    An *unnamed* session has no document naming its Pod,
    which is all a ``push-servers`` token without ``admin:server_state`` leaves.
    The rest of the unmeasured sessions named a Pod
    that Prometheus held no samples for over the session's span.
    """

    started: int = 0
    unnamed: int = 0
    measured: int = 0

    @property
    def no_samples(self) -> int:
        """How many sessions named a Pod but could not be measured."""
        return self.started - self.unnamed - self.measured


@dataclass(frozen=True)
class _PodSpan:
    """A session's Pod, and the epoch seconds the report credits to it."""

    namespace: str
    pod: str
    start: int
    end: int


def _named_pod(doc: dict[str, Any]) -> tuple[str, str] | None:
    """Return the ``(namespace, pod)`` a document names, else ``None``."""
    namespace = doc.get("server.state.namespace")
    pod = doc.get("server.state.pod_name")
    if isinstance(namespace, str) and namespace and isinstance(pod, str) and pod:
        return namespace, pod
    return None


def _pod_spans(
    documents: Iterable[dict[str, Any]],
) -> tuple[list[SessionKey], dict[SessionKey, _PodSpan]]:
    """Return the started sessions, and a span for each one naming its Pod.

    A span ends at the session's last snapshot
    and begins one interval before its first,
    which is the stretch the report credits the session's time to,
    but never before ``server.started``:
    KubeSpawner gives a server's Pod the same name on every start,
    so a span reaching back past this one
    could find the previous session's Pod instead.

    A session still spawning has no start to bound its span by,
    so it is not among the started sessions.
    """
    started: dict[SessionKey, None] = {}
    spans: dict[SessionKey, _PodSpan] = {}

    for key, seconds, doc in session_contributions(documents):
        if not key[2]:
            continue
        started[key] = None

        named = _named_pod(doc)
        snapshot = snapshot_seconds(doc)
        if named is None or snapshot is None:
            continue

        start = int(snapshot - seconds)
        floor = started_seconds(doc)
        if floor is not None:
            start = max(start, floor)
        span = _PodSpan(namespace=named[0], pod=named[1], start=start, end=snapshot)

        previous = spans.get(key)
        if previous is not None:
            span = replace(
                previous,
                start=min(previous.start, span.start),
                end=max(previous.end, span.end),
            )
        spans[key] = span

    return list(started), spans


def _peak_query(span: _PodSpan) -> str:
    """Build the PromQL for a Pod's peak working set over *span*.

    The query is evaluated at the span's end, and looks back over its length.
    The matchers on ``container`` and ``image``
    leave out the cgroup series that already total the Pod's containers,
    so that the sum does not count them twice.
    The label values are quoted as JSON strings,
    whose escapes PromQL also reads.
    """
    selector = ",".join(
        [
            f"namespace={json.dumps(span.namespace)}",
            f"pod={json.dumps(span.pod)}",
            'container!=""',
            'image!=""',
        ]
    )
    seconds = max(span.end - span.start, 1)
    return f"max_over_time(sum(container_memory_working_set_bytes{{{selector}}})[{seconds}s:])"


def measure_peak_memory(
    documents: Iterable[dict[str, Any]],
    client: MetricsQuerier,
) -> tuple[dict[SessionKey, float], MemoryTally]:
    """Query the peak memory of each started session's Pod, in bytes.

    Each session costs one query, over the span :func:`_pod_spans` gives it.
    A session that names no Pod, or whose Pod has no samples in that span,
    is left out of the mapping; the tally says which of the two it was.

    Raises:
        ExternalServiceError: If a query fails.
    """
    started, spans = _pod_spans(documents)

    peaks: dict[SessionKey, float] = {}
    for key, span in spans.items():
        result = client.query(_peak_query(span), time=span.end)
        if result:
            peaks[key] = float(result[0]["value"][1])

    tally = MemoryTally(
        started=len(started), unnamed=len(started) - len(spans), measured=len(peaks)
    )
    return peaks, tally
