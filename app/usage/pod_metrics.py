"""Peak memory and CPU time per session, read from Prometheus.

A session runs in a KubeSpawner Pod, which the spawner names in its state.
``push-servers`` records that state as ``server.state.pod_name``
and ``server.state.namespace``
when its JupyterHub token holds the ``admin:server_state`` scope.

Both figures are summed over the Pod's containers.
The peak is taken from ``container_memory_working_set_bytes``,
the figure the kubelet holds a container's memory limit against,
and the CPU time from ``container_cpu_usage_seconds_total``.
"""

import json
from collections.abc import Callable, Iterable, Mapping
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
class PodTally:
    """How many started sessions a run measured, and why it missed the rest.

    An *unnamed* session has no document naming its Pod,
    which is all a ``push-servers`` token without ``admin:server_state`` leaves.
    The rest of the unmeasured sessions named a Pod
    that Prometheus held too few samples for over the session's span.
    """

    started: int = 0
    unnamed: int = 0
    measured: int = 0

    @property
    def too_few_samples(self) -> int:
        """How many sessions named a Pod but could not be measured."""
        return self.started - self.unnamed - self.measured


@dataclass(frozen=True)
class PodMetrics:
    """The figures measured for each session, keyed by the session.

    *cpu_time* is in seconds, and *peak_memory* in bytes.
    A session absent from a mapping could not be measured.
    """

    cpu_time: Mapping[SessionKey, float]
    peak_memory: Mapping[SessionKey, float]


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


def _pod_selector(span: _PodSpan) -> str:
    """Return the label matchers for the containers of *span*'s Pod.

    The matchers on ``container`` and ``image``
    leave out the cgroup series that already total the Pod's containers,
    so that a sum over the selected series does not count them twice.
    The label values are quoted as JSON strings,
    whose escapes PromQL also reads.
    """
    return ",".join(
        [
            f"namespace={json.dumps(span.namespace)}",
            f"pod={json.dumps(span.pod)}",
            'container!=""',
            'image!=""',
        ]
    )


def _span_seconds(span: _PodSpan) -> int:
    """Return *span*'s length in seconds, at least 1, as a PromQL range needs."""
    return max(span.end - span.start, 1)


def _peak_memory_query(span: _PodSpan) -> str:
    """Build the PromQL for a Pod's peak working set over *span*."""
    selector = _pod_selector(span)
    return (
        f"max_over_time(sum(container_memory_working_set_bytes{{{selector}}})"
        f"[{_span_seconds(span)}s:])"
    )


def _cpu_time_query(span: _PodSpan) -> str:
    """Build the PromQL for the CPU time a Pod used over *span*.

    ``increase`` reads through a counter reset, such as a restarted container's,
    but needs at least two samples of a series within *span*.
    """
    selector = _pod_selector(span)
    return f"sum(increase(container_cpu_usage_seconds_total{{{selector}}}[{_span_seconds(span)}s]))"


def _measure(
    documents: Iterable[dict[str, Any]],
    client: MetricsQuerier,
    build_query: Callable[[_PodSpan], str],
) -> tuple[dict[SessionKey, float], PodTally]:
    """Evaluate *build_query* for each started session's Pod.

    Each session costs one query, over the span :func:`_pod_spans` gives it,
    evaluated at the span's end.
    A session that names no Pod, or whose Pod has too few samples in that span,
    is left out of the mapping; the tally says which of the two it was.

    Raises:
        ExternalServiceError: If a query fails.
    """
    started, spans = _pod_spans(documents)

    values: dict[SessionKey, float] = {}
    for key, span in spans.items():
        result = client.query(build_query(span), time=span.end)
        if result:
            values[key] = float(result[0]["value"][1])

    tally = PodTally(
        started=len(started), unnamed=len(started) - len(spans), measured=len(values)
    )
    return values, tally


def measure_peak_memory(
    documents: Iterable[dict[str, Any]],
    client: MetricsQuerier,
) -> tuple[dict[SessionKey, float], PodTally]:
    """Query the peak memory of each started session's Pod, in bytes.

    See :func:`_measure` for which sessions the mapping leaves out.

    Raises:
        ExternalServiceError: If a query fails.
    """
    return _measure(documents, client, _peak_memory_query)


def measure_cpu_time(
    documents: Iterable[dict[str, Any]],
    client: MetricsQuerier,
) -> tuple[dict[SessionKey, float], PodTally]:
    """Query the CPU time each started session's Pod used, in seconds.

    See :func:`_measure` for which sessions the mapping leaves out.

    Raises:
        ExternalServiceError: If a query fails.
    """
    return _measure(documents, client, _cpu_time_query)
