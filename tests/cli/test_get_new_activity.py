"""Tests for :mod:`app.cli.get_new_activity`.

Accounting for the activity and assembling the report belong to
:mod:`app.usage.activity` and :mod:`app.reports.builders`, and are covered
through those. What is left here is the query the command sends, how it
wires the window and its output flags into those modules, and its own
stderr diagnostic: the wording it gives each shape of document set, and
the fact that it stays off stdout, where the report goes.
"""

# This module exercises the command's internal helpers directly.
# pyright: reportPrivateUsage=false

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from app.cli import get_new_activity
from app.cli.get_new_activity import _build_parser, _format_document_tally, _run
from app.core.time_utils import DATETIME_FMT
from app.reports.builders import build_activity_report
from app.reports.delivery import OutputTargets
from app.reports.model import Report
from app.reports.renderers import render_text
from app.usage.activity import (
    DocumentTally,
    SkipReason,
    compute_activity,
    summarize_activity,
)
from tests.helpers.stub_clients import QueryStubClient, install_client

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

# The explicit reporting window the run tests resolve, in the default
# timezone, so that a report built from the same bounds can be compared
# against the one the command prints.
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
    *argv: str,
) -> str:
    """Run the command over that window and return the subject it sends.

    The delivery step is replaced rather than the SMTP transport, so the
    assertion reaches the subject the command composed, not the wording
    of a header.
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
            *argv,
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

    The query's hub term can also return a hub whose name merely contains
    the one asked for, so the command keeps the exact match alone.
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


def test_the_report_on_stdout_is_the_builders_over_the_resolved_window(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The window, the summary, and the output flags all reach the builders.

    The report is compared against one built from the same documents, so
    a flag reaching the wrong parameter, or a timestamp format other than
    the one this command fixes, would show up as a different report.
    """
    documents = [_document("a@x.edu"), _document("b@x.edu")]
    assert _report_on(monkeypatch, documents, "--detailed-usernames") == 0

    sessions = compute_activity(documents)
    expected = build_activity_report(
        sessions=sessions,
        summary=summarize_activity(sessions, documents, CHICAGO, WINDOW_START, WINDOW_END),
        start_time=WINDOW_START,
        end_time=WINDOW_END,
        strftime_fmt=DATETIME_FMT,
        detailed_usernames=True,
        anonymize=False,
    )
    assert capsys.readouterr().out == f"---\n{render_text(expected)}\n---\n"


def test_a_clean_run_reports_what_it_counted() -> None:
    """Nothing was skipped, so no reason has to be named."""
    tally = DocumentTally(retrieved=1440, counted=1440)
    assert _format_document_tally(tally) == "Retrieved 1440 documents; counted 1440."


def test_documents_retrieved_and_then_discarded_say_why() -> None:
    """Without the reason this reads exactly like a hub that was idle."""
    tally = DocumentTally(retrieved=1440, counted=0, skipped={SkipReason.NO_INTERVAL: 1440})
    assert _format_document_tally(tally) == (
        "Retrieved 1440 documents; counted 0, skipped 1440 (no meta.interval: 1440)."
    )


def test_retrieving_nothing_is_not_reported_as_a_skip() -> None:
    """An idle window, a wrong index, and an unmatched hub all land here."""
    assert _format_document_tally(DocumentTally()) == "Retrieved 0 documents."


def test_a_single_document_is_not_pluralized() -> None:
    """The count carries its own noun."""
    tally = DocumentTally(retrieved=1, counted=1)
    assert _format_document_tally(tally) == "Retrieved 1 document; counted 1."


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


def test_the_diagnostic_goes_to_stderr_and_leaves_stdout_to_the_report(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The report stays safe to pipe, so the diagnostic joins the window echo."""
    documents = [_document("a@x.edu"), {"user.name": "b@x.edu"}]
    assert _report_on(monkeypatch, documents) == 0

    captured = capsys.readouterr()
    diagnostic = "Retrieved 2 documents; counted 1, skipped 1 (no meta.interval: 1)."
    assert diagnostic in captured.err.splitlines()
    assert "Reporting window:" in captured.err
    assert "Retrieved" not in captured.out
    assert captured.out.startswith("---\n")


def test_the_diagnostic_does_not_change_the_command_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An empty window is legitimate, so a zero report still succeeds."""
    assert _report_on(monkeypatch, []) == 0
    assert "Retrieved 0 documents." in capsys.readouterr().err.splitlines()
