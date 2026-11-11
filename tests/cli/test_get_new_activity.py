"""Tests for :mod:`jupyterhub_monitoring.cli.get_new_activity`.

Accounting for the activity, measuring its Pods, and assembling the report
belong to :mod:`jupyterhub_monitoring.usage.activity`,
:mod:`jupyterhub_monitoring.usage.pod_metrics`,
and :mod:`jupyterhub_monitoring.reports.builders`,
and are covered through those.
What is left here is the query the command sends,
how it wires the window, its output flags, and Prometheus into those modules,
and its own stderr diagnostics:
the wording it gives each shape of document set and of measurement,
and the fact that they stay off stdout, where the report states them again.
"""

# This module exercises the command's internal helpers directly.
# pyright: reportPrivateUsage=false

from datetime import datetime
from typing import Any, override
from zoneinfo import ZoneInfo

import pytest

from jupyterhub_monitoring.cli import get_new_activity
from jupyterhub_monitoring.cli.get_new_activity import (
    _build_parser,
    _format_document_tally,
    _format_pod_tally,
    _run,
)
from jupyterhub_monitoring.core.time_utils import DATETIME_FMT
from jupyterhub_monitoring.reports.builders import build_activity_report
from jupyterhub_monitoring.reports.delivery import OutputTargets
from jupyterhub_monitoring.reports.model import Report
from jupyterhub_monitoring.reports.renderers import render_text
from jupyterhub_monitoring.usage.activity import (
    DocumentTally,
    SkipReason,
    compute_activity,
    summarize_activity,
    tally_documents,
)
from jupyterhub_monitoring.usage.pod_metrics import PodMetrics, PodTally
from tests.helpers.stub_clients import (
    PrometheusStubClient,
    QueryStubClient,
    install_client,
    install_prometheus_client,
)

# The email settings a run needs before it will build any at all.
_EMAIL_ARGV = [
    "--send-email",
    "--sender-email",
    "reports@example.edu",
    "--recipient-email",
    "ops@example.edu",
    "--smtp-host",
    "smtp.example.edu",
    "--smtp-port",
    "465",
]

# The explicit reporting window the run tests resolve,
# in the default timezone, so that a report built from the same bounds
# can be compared against the one the command prints.
CHICAGO = ZoneInfo("America/Chicago")
WINDOW_START = datetime(2026, 8, 1, tzinfo=CHICAGO)
WINDOW_END = datetime(2026, 9, 1, tzinfo=CHICAGO)


def _document(user: str) -> dict[str, Any]:
    """Build a countable snapshot of *user*'s server, inside the window."""
    return {
        "user.name": user,
        "server.name": "",
        "server.ready": True,
        "server.started": "2026-08-05T14:00:00Z",
        "meta.snapshot-time": int(datetime(2026, 8, 5, 10, 0, tzinfo=CHICAGO).timestamp()),
        "meta.interval": "5m",
    }


class _PerMetricPrometheus(PrometheusStubClient):  # pylint: disable=too-few-public-methods
    """Answers a Pod's CPU-time query with a value of its own.

    The base stub answers every metric alike,
    which could not tell a run that crossed the two figures from one that did not.
    """

    def __init__(self, values: dict[str, float], cpu_seconds: float) -> None:
        """Store the value to answer a CPU-time query with."""
        super().__init__(values)
        self.cpu_seconds = cpu_seconds

    @override
    def query(self, promql: str, time: int) -> list[dict[str, Any]]:
        """Answer as the base does, but with *cpu_seconds* for a CPU-time query."""
        result = super().query(promql, time)
        if result and "container_cpu_usage_seconds_total" in promql:
            return [{"metric": {}, "value": [time, str(self.cpu_seconds)]}]
        return result


def _report_on(
    monkeypatch: pytest.MonkeyPatch,
    documents: list[dict[str, Any]],
    *argv: str,
) -> int:
    """Run the command over *documents*, against a stubbed client."""
    client = QueryStubClient(documents)
    install_client(monkeypatch, get_new_activity, client)
    args = _build_parser().parse_args(
        [
            "--es-endpoint",
            "https://es.example.edu:9200",
            "--es-index",
            "servers",
            "--report-start",
            "2026-08-01 00:00",
            "--report-end",
            "2026-09-01 00:00",
            *argv,
        ]
    )
    return _run(args)


def _emailed_subject(
    monkeypatch: pytest.MonkeyPatch,
    report_start: str,
    report_end: str,
) -> str:
    """Run the command over that window and return the subject it sends.

    The delivery step is replaced rather than the SMTP transport,
    so the assertion reaches the subject the command composed,
    not the wording of a header.
    """
    captured: list[OutputTargets] = []

    def _deliver(targets: OutputTargets, _report: Report) -> int:
        captured.append(targets)
        return 0

    install_client(monkeypatch, get_new_activity, QueryStubClient([]))
    monkeypatch.setattr(get_new_activity, "deliver_report", _deliver)
    args = _build_parser().parse_args(
        [
            "--es-endpoint",
            "https://es.example.edu:9200",
            "--es-index",
            "servers",
            "--report-start",
            report_start,
            "--report-end",
            report_end,
            *_EMAIL_ARGV,
        ]
    )
    assert _run(args) == 0
    email = captured[0].email
    assert email is not None
    return email.subject


def test_the_query_covers_the_reporting_window_and_the_hub(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The documents counted are the ones the window and --hub select.

    The query's hub term can also return a hub
    whose name merely contains the one asked for,
    so the command keeps the exact match alone.
    """
    document = {"user.name": "a@x.edu", "meta.interval": "5m"}
    client = QueryStubClient(
        [document | {"meta.hub": "production"}, document | {"meta.hub": "production-east"}]
    )
    install_client(monkeypatch, get_new_activity, client)
    args = _build_parser().parse_args(
        [
            "--es-endpoint",
            "https://es.example.edu:9200",
            "--es-index",
            "servers",
            "--report-start",
            "2026-08-01 00:00",
            "--report-end",
            "2026-09-01 00:00",
            "--hub",
            "production",
        ]
    )
    assert _run(args) == 0

    assert client.request is not None
    assert client.request["index"] == "servers"
    filters = client.request["query"]["bool"]["filter"]
    chicago = ZoneInfo("America/Chicago")
    assert {
        "range": {
            "meta.snapshot-time": {
                "gte": int(datetime(2026, 8, 1, tzinfo=chicago).timestamp()),
                "lt": int(datetime(2026, 9, 1, tzinfo=chicago).timestamp()),
            }
        }
    } in filters
    assert any("production" in str(f) for f in filters)
    assert "Retrieved 1 document; counted 1." in capsys.readouterr().err.splitlines()


@pytest.mark.parametrize(
    ("flag", "detailed_usernames", "anonymize"),
    [("--detailed-usernames", True, False), ("--anonymize", False, True)],
)
def test_the_report_on_stdout_is_the_builders_over_the_resolved_window(
    flag: str,
    detailed_usernames: bool,
    anonymize: bool,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The window, summary, tally, and output flags all reach the builders.

    The report is compared against one built from the same documents,
    so a flag reaching the wrong parameter,
    or a timestamp format other than the one this command fixes,
    would show up as a different report.
    One document is skipped, so a tally other than the documents' own
    would too.
    """
    documents = [_document("a@x.edu"), _document("b@x.edu"), {"user.name": "c@x.edu"}]
    assert _report_on(monkeypatch, documents, flag) == 0

    sessions = compute_activity(documents)
    expected = build_activity_report(
        sessions=sessions,
        summary=summarize_activity(sessions, documents, CHICAGO, WINDOW_START, WINDOW_END),
        tally=tally_documents(documents),
        start_time=WINDOW_START,
        end_time=WINDOW_END,
        strftime_fmt=DATETIME_FMT,
        detailed_usernames=detailed_usernames,
        anonymize=anonymize,
    )
    assert capsys.readouterr().out == f"---\n{render_text(expected)}\n---\n"


def test_each_sessions_pod_metrics_reach_the_report(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The figures Prometheus reports for a Pod land on that Pod's session.

    The report is compared against one built with the same figures,
    so a figure attached to the wrong session would show up as a different report.
    The session whose snapshots name no Pod is counted on stderr instead.
    """
    pod = {"server.state.pod_name": "jupyter-a", "server.state.namespace": "jhub"}
    documents = [_document("a@x.edu") | pod, _document("b@x.edu")]
    install_prometheus_client(
        monkeypatch, get_new_activity, _PerMetricPrometheus({"jupyter-a": 2**30}, 120.0)
    )
    argv = ["--prometheus-endpoint", "https://prometheus.example.edu"]
    assert _report_on(monkeypatch, documents, *argv) == 0

    sessions = compute_activity(documents)
    key = ("a@x.edu", "", "2026-08-05T14:00:00Z")
    expected = build_activity_report(
        sessions=sessions,
        summary=summarize_activity(sessions, documents, CHICAGO, WINDOW_START, WINDOW_END),
        tally=tally_documents(documents),
        start_time=WINDOW_START,
        end_time=WINDOW_END,
        strftime_fmt=DATETIME_FMT,
        pod_metrics=PodMetrics(cpu_time={key: 120.0}, peak_memory={key: float(2**30)}),
    )
    captured = capsys.readouterr()
    assert captured.out == f"---\n{render_text(expected)}\n---\n"
    errors = captured.err.splitlines()
    assert "Measured CPU time for 1 of 2 started sessions; 1 named no Pod." in errors
    assert "Measured peak memory for 1 of 2 started sessions; 1 named no Pod." in errors


def test_the_pod_diagnostic_tells_its_two_gaps_apart() -> None:
    """A missing token scope and a Prometheus short of samples need different fixes."""
    tally = PodTally(started=5, unnamed=1, measured=3)
    assert _format_pod_tally("peak memory", tally) == (
        "Measured peak memory for 3 of 5 started sessions; "
        "1 named no Pod, 1 had too few samples."
    )


def test_a_clean_run_reports_what_it_counted() -> None:
    """Nothing was skipped, so no reason has to be named."""
    tally = DocumentTally(retrieved=1440, counted=1440)
    assert _format_document_tally(tally) == "Retrieved 1440 documents; counted 1440."


def test_the_reasons_are_listed_in_a_fixed_order() -> None:
    """Reason order follows the rule's own checks, not the documents'."""
    tally = DocumentTally(
        retrieved=11,
        counted=1,
        skipped={
            SkipReason.BAD_INTERVAL: 3,
            SkipReason.NO_USER: 1,
            SkipReason.NON_POSITIVE_INTERVAL: 4,
            SkipReason.NO_INTERVAL: 2,
        },
    )
    assert _format_document_tally(tally) == (
        "Retrieved 11 documents; counted 1, skipped 10"
        " (no user.name: 1, no meta.interval: 2, unparsable meta.interval: 3,"
        " non-positive meta.interval: 4)."
    )


def test_the_default_subject_names_this_report_and_one_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A daily send is told apart in an inbox list by its date alone."""
    subject = _emailed_subject(monkeypatch, "2026-09-01 00:00", "2026-09-02 00:00")
    assert subject == "JupyterHub Activity Report: 2026-09-01"


def test_the_diagnostic_does_not_change_the_command_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An empty window is legitimate, so a zero report still succeeds."""
    assert _report_on(monkeypatch, []) == 0
    assert "Retrieved 0 documents." in capsys.readouterr().err.splitlines()


def test_a_failed_delivery_fails_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """The exit code is delivery's own, so an unsent report is not a success."""

    def _fail(_targets: OutputTargets, _report: Report) -> int:
        return 1

    monkeypatch.setattr(get_new_activity, "deliver_report", _fail)
    assert _report_on(monkeypatch, []) == 1
