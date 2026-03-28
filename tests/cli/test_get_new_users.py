"""Tests for :mod:`app.cli.get_new_users`.

Selecting the users and dating their servers is :mod:`app.usage.new_users`'
work, covered by ``tests/usage/test_new_users.py``. What is left here is
the command's own Elasticsearch lookup, whose window is not the reporting
window and which ``--hub`` scopes, and its stderr tally of the user list
the hub answered with.
"""

# This module exercises the command's internal helpers directly.
# pyright: reportPrivateUsage=false

import argparse
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from app.cli import get_new_users
from app.cli.get_new_users import (
    _build_parser,
    _format_user_tally,
    _query_first_server,
    _run,
    _validate_arguments,
)
from app.core.time_utils import DATE_FMT
from app.reports.builders import build_new_users_report
from app.reports.delivery import OutputTargets
from app.reports.model import Report
from app.reports.renderers import render_text
from app.usage.new_users import filter_new_users

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

START = datetime(2026, 7, 1, tzinfo=timezone.utc)

# The explicit reporting window the tally tests run against, in the
# default timezone, so that the report they compare against is built from
# the same bounds the command resolves rather than from the clock.
CHICAGO = ZoneInfo("America/Chicago")
WINDOW_START = datetime(2026, 8, 1, tzinfo=CHICAGO)
WINDOW_END = datetime(2026, 9, 1, tzinfo=CHICAGO)


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


def _lookup(
    monkeypatch: pytest.MonkeyPatch,
    documents: list[dict[str, Any]],
    hub: str | None = None,
) -> tuple[dict[str, int], _StubClient]:
    """Run the lookup against a stubbed client."""
    client = _StubClient(documents)

    def _make_client(_args: argparse.Namespace) -> _StubClient:
        return client

    monkeypatch.setattr(get_new_users, "make_es_client", _make_client)
    args = argparse.Namespace(es_index="servers", hub=hub)
    return _query_first_server(args, START), client


def _hub_filters(client: _StubClient) -> list[dict[str, Any]]:
    """Return the recorded query's clauses that mention ``meta.hub``."""
    assert client.request is not None
    filters = client.request["query"]["bool"]["filter"]
    return [f for f in filters if "meta.hub" in str(f)]


def _window(client: _StubClient) -> dict[str, int]:
    """Return the snapshot-time range the recorded query filtered on."""
    assert client.request is not None
    filters = client.request["query"]["bool"]["filter"]
    ranges = [f["range"]["meta.snapshot-time"] for f in filters if "range" in f]
    assert len(ranges) == 1
    return ranges[0]


def test_the_lookup_runs_from_the_window_start_through_now(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new user cannot have started a server before being created.

    The search therefore extends past the end of the reporting window, so
    a server first started after the window closed is still reported.
    """
    before = int(datetime.now(timezone.utc).timestamp())
    _, client = _lookup(monkeypatch, [])
    after = int(datetime.now(timezone.utc).timestamp())

    window = _window(client)
    assert window["gte"] == int(START.timestamp())
    assert before <= window["lt"] <= after


def test_the_command_accepts_the_same_hub_filter_as_get_new_activity() -> None:
    """The two report commands spell the option the same way."""
    args = _build_parser().parse_args(
        ["--jupyterhub-endpoint", "https://hub.example.edu/hub/api", "--hub", "production"]
    )
    assert args.hub == "production"


def test_a_hub_without_elasticsearch_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--hub scopes the lookup alone, so on its own it would do nothing."""
    monkeypatch.setenv("JUPYTERHUB_API_KEY", "key")
    parser = _build_parser()
    args = parser.parse_args(
        [
            "--jupyterhub-endpoint",
            "https://hub.example.edu/hub/api",
            "--duration",
            "7 days",
            "--hub",
            "production",
        ]
    )
    with pytest.raises(SystemExit) as exit_info:
        _validate_arguments(args, parser)
    assert exit_info.value.code == 2
    assert "--hub requires --es-endpoint" in capsys.readouterr().err


def test_the_lookup_is_scoped_to_the_given_hub(monkeypatch: pytest.MonkeyPatch) -> None:
    """--hub reaches the query, so only that hub dates a user's first server."""
    _, client = _lookup(monkeypatch, [], hub="production")
    assert _hub_filters(client) == [
        {
            "bool": {
                "should": [
                    {"term": {"meta.hub.keyword": "production"}},
                    {"term": {"meta.hub": "production"}},
                ],
                "minimum_should_match": 1,
            }
        }
    ]


def test_the_lookup_spans_every_hub_without_one(monkeypatch: pytest.MonkeyPatch) -> None:
    """Omitting --hub draws from the whole index, as it always has."""
    _, client = _lookup(monkeypatch, [])
    assert _hub_filters(client) == []
    assert client.request is not None
    assert client.request["index"] == "servers"


def test_each_user_gets_their_earliest_server_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The documents go straight to :func:`compute_first_server`."""
    documents: list[dict[str, Any]] = [
        {"user.name": "a@x.edu", "server.started": "2026-07-04T10:00:00Z"},
        {"user.name": "a@x.edu", "server.started": "2026-07-02T08:00:00Z"},
        {"user.name": "b@x.edu", "meta.snapshot-time": 1_783_000_000},
    ]
    first_server, _ = _lookup(monkeypatch, documents)
    assert first_server == {
        "a@x.edu": int(datetime(2026, 7, 2, 8, tzinfo=timezone.utc).timestamp()),
        "b@x.edu": 1_783_000_000,
    }


class _StubHub:  # pylint: disable=too-few-public-methods
    """Answers ``list_users`` with a prepared user list."""

    def __init__(self, users: list[dict[str, Any]]) -> None:
        """Store the users to yield."""
        self.users = users

    def list_users(self) -> list[dict[str, Any]]:
        """Return the prepared users."""
        return list(self.users)


def _report_on(
    monkeypatch: pytest.MonkeyPatch,
    users: list[dict[str, Any]],
) -> int:
    """Run the command over *users*, against a stubbed hub and no index."""

    def _make_client(_args: argparse.Namespace) -> _StubHub:
        return _StubHub(users)

    monkeypatch.setattr(get_new_users, "make_jupyterhub_client", _make_client)
    args = _build_parser().parse_args(
        [
            "--jupyterhub-endpoint",
            "https://hub.example.edu/hub/api",
            "--report-start",
            "2026-08-01 00:00",
            "--report-end",
            "2026-09-01 00:00",
        ]
    )
    return _run(args)


def test_a_single_user_is_not_pluralized() -> None:
    """The count carries its own noun, as the document tally's does."""
    assert _format_user_tally(1, 1) == "Retrieved 1 user; 1 created in the reporting window."


def test_several_users_are_pluralized() -> None:
    """The wording matches ``get-new-activity``'s, so the two read alike."""
    assert _format_user_tally(812, 3) == (
        "Retrieved 812 users; 3 created in the reporting window."
    )


def test_retrieving_no_users_does_not_claim_none_of_none_were_created() -> None:
    """A hub that answered with nothing is the failure worth seeing."""
    assert _format_user_tally(0, 0) == "Retrieved 0 users."


def test_the_tally_counts_only_the_users_created_in_the_window(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The second number is the report's own selection, not the list's size.

    The report on stdout is compared against one built from the same
    users, so the tally is shown to have changed no figure in it.
    """
    users: list[dict[str, Any]] = [
        {"name": "a@x.edu", "created": "2026-08-05T10:00:00Z"},
        {"name": "b@x.edu", "created": "2026-08-06T11:00:00Z"},
        {"name": "old@x.edu", "created": "2020-01-01T00:00:00Z"},
        {"name": "later@x.edu", "created": "2026-09-15T00:00:00Z"},
    ]
    assert _report_on(monkeypatch, users) == 0

    captured = capsys.readouterr()
    assert "Retrieved 4 users; 2 created in the reporting window." in captured.err.splitlines()
    assert "Reporting window:" in captured.err
    assert "Retrieved" not in captured.out

    expected = build_new_users_report(
        users=filter_new_users(users, WINDOW_START, WINDOW_END),
        start_time=WINDOW_START,
        end_time=WINDOW_END,
        strftime_fmt=DATE_FMT,
        detailed_usernames=False,
        first_server=None,
        anonymize=False,
    )
    assert captured.out == f"---\n{render_text(expected)}\n---\n"


def test_a_hub_that_answered_with_no_users_still_succeeds(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An empty user list is legitimate, so the exit code does not move."""
    assert _report_on(monkeypatch, []) == 0
    assert "Retrieved 0 users." in capsys.readouterr().err.splitlines()


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

    def _make_client(_args: argparse.Namespace) -> _StubHub:
        return _StubHub([])

    def _deliver(targets: OutputTargets, _report: Report) -> int:
        captured.append(targets)
        return 0

    monkeypatch.setattr(get_new_users, "make_jupyterhub_client", _make_client)
    monkeypatch.setattr(get_new_users, "deliver_report", _deliver)
    args = _build_parser().parse_args(
        [
            "--jupyterhub-endpoint",
            "https://hub.example.edu/hub/api",
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


def test_the_default_subject_names_this_report_and_one_day(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The prefix is this command's own, not the activity report's."""
    subject = _emailed_subject(monkeypatch, "2026-09-01 00:00", "2026-09-02 00:00")
    assert subject == "JupyterHub New Users Report: 2026-09-01"


def test_the_default_subject_names_this_report_and_a_longer_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A window of several days names the first and the last of them."""
    subject = _emailed_subject(monkeypatch, "2026-08-27 00:00", "2026-09-02 00:00")
    assert subject == "JupyterHub New Users Report: 2026-08-27 to 2026-09-01"


def test_an_explicit_subject_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """Whoever names the subject means it, window or no window."""
    subject = _emailed_subject(
        monkeypatch, "2026-08-27 00:00", "2026-09-02 00:00", "--subject", "Weekly signups"
    )
    assert subject == "Weekly signups"
