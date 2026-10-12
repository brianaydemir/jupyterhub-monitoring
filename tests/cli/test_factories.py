"""Tests for :mod:`app.cli.factories`.

The clients are replaced with recorders,
because the factory's job is to map an ``argparse.Namespace``
onto constructor arguments, not to connect to anything.
"""

import argparse
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from app.cli import factories
from app.cli.factories import (
    compute_report_time_range,
    get_strftime_fmt,
    make_es_client,
    make_jupyterhub_client,
    make_output_targets,
)
from app.core import time_utils
from app.core.errors import AuthCancelledError, DataShapeError
from app.core.time_utils import DATE_FMT, DATETIME_FMT
from app.reports.delivery import OutputTargets
from tests.helpers.clock import FrozenClock

CHICAGO = ZoneInfo("America/Chicago")


@pytest.fixture(autouse=True)
def without_ambient_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the host's own API keys from deciding a case's outcome."""
    monkeypatch.delenv("JUPYTERHUB_API_KEY", raising=False)
    monkeypatch.delenv("ELASTICSEARCH_API_KEY", raising=False)


def _recorder(recorded: list[dict[str, Any]]) -> Callable[..., object]:
    """Return a stand-in client class that records its keyword arguments."""

    def build(**kwargs: Any) -> object:
        recorded.append(kwargs)
        return object()

    return build


#
# make_es_client
#


def _es_args(
    *,
    api_key: Path | None = None,
    ca_cert: Path | None = None,
    username: str | None = None,
) -> argparse.Namespace:
    """Build the namespace :func:`make_es_client` reads."""
    return argparse.Namespace(
        es_endpoint="https://es.example.edu:9200",
        es_api_key=api_key,
        es_ca_cert=ca_cert,
        es_username=username,
    )


def test_an_es_client_takes_its_api_key_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No username means no prompt, so the key must come from a source."""
    monkeypatch.setenv("ELASTICSEARCH_API_KEY", "secret")
    recorded: list[dict[str, Any]] = []
    monkeypatch.setattr(factories, "ElasticsearchClient", _recorder(recorded))

    make_es_client(_es_args(ca_cert=Path("/certs/ca.pem")))

    assert recorded == [
        {
            "endpoint": "https://es.example.edu:9200",
            "api_key": "secret",
            "ca_cert": Path("/certs/ca.pem"),
        }
    ]


def test_an_es_client_prompts_when_a_username_is_given(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The password never comes from an argument, only from a prompt."""
    recorded: list[dict[str, Any]] = []
    monkeypatch.setattr(factories, "ElasticsearchClient", _recorder(recorded))
    monkeypatch.setattr(factories, "prompt_password", lambda: "pw")

    make_es_client(_es_args(username="elastic"))

    assert recorded == [
        {
            "endpoint": "https://es.example.edu:9200",
            "basic_auth": ("elastic", "pw"),
            "ca_cert": None,
        }
    ]


def test_cancelling_the_password_prompt_is_an_expected_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ctrl-C at the prompt should exit 1, not raise an unexpected error."""
    monkeypatch.setattr(factories, "prompt_password", lambda: None)

    with pytest.raises(AuthCancelledError):
        make_es_client(_es_args(username="elastic"))


def test_an_es_client_without_any_credential_is_rejected() -> None:
    """Validation normally catches this; the factory does not guess."""
    with pytest.raises(DataShapeError):
        make_es_client(_es_args())


#
# make_jupyterhub_client
#


def test_a_jupyterhub_client_reads_its_key_from_a_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The file's trailing newline is stripped before it reaches the client."""
    api_key = tmp_path / "key.txt"
    api_key.write_text("secret\n", encoding="utf-8")
    recorded: list[dict[str, Any]] = []
    monkeypatch.setattr(factories, "JupyterHubClient", _recorder(recorded))

    make_jupyterhub_client(
        argparse.Namespace(
            jupyterhub_endpoint="https://hub.example.edu/hub/api",
            jupyterhub_api_key=api_key,
            jupyterhub_ca_cert=None,
        )
    )

    assert recorded == [
        {
            "endpoint": "https://hub.example.edu/hub/api",
            "api_key": "secret",
            "ca_cert": None,
        }
    ]


#
# make_output_targets
#


# A day-long window and a week-long one,
# both in the zone the report commands default to,
# so the dates a subject names are read the way the report reads them.
ONE_DAY = (datetime(2026, 9, 2, tzinfo=CHICAGO), datetime(2026, 9, 3, tzinfo=CHICAGO))
ONE_WEEK = (datetime(2026, 8, 27, tzinfo=CHICAGO), datetime(2026, 9, 3, tzinfo=CHICAGO))

SUBJECT_PREFIX = "JupyterHub Activity Report"


def _output_args(
    *,
    send_email: bool = False,
    recipient_email: list[str] | None = None,
    subject: str | None = None,
) -> argparse.Namespace:
    """Build the namespace :func:`make_output_targets` reads."""
    return argparse.Namespace(
        text_file=Path("out.txt"),
        html_file=None,
        csv_file=None,
        xlsx_file=Path("out.xlsx"),
        send_email=send_email,
        sender_name="Reports",
        sender_email="reports@example.edu",
        recipient_email=recipient_email or ["ops@example.edu"],
        subject=subject,
        smtp_host="smtp.example.edu",
        smtp_port=465,
        smtp_no_ssl=False,
    )


def _targets(
    args: argparse.Namespace,
    window: tuple[datetime, datetime] = ONE_DAY,
) -> OutputTargets:
    """Build the delivery targets for *args* over *window*."""
    return make_output_targets(args, subject_prefix=SUBJECT_PREFIX, window=window)


def test_output_targets_carry_the_requested_files() -> None:
    """A flag that was not given stays ``None``."""
    targets = _targets(_output_args())
    assert targets.text_file == Path("out.txt")
    assert targets.xlsx_file == Path("out.xlsx")
    assert targets.html_file is None
    assert targets.csv_file is None


def test_email_settings_are_collected_only_with_send_email() -> None:
    """The destination and its configuration cannot disagree."""
    assert _targets(_output_args()).email is None

    email = _targets(_output_args(send_email=True)).email
    assert email is not None
    assert email.sender_name == "Reports"
    assert email.sender_email == "reports@example.edu"
    assert email.recipients == ("ops@example.edu",)
    assert email.subject == "JupyterHub Activity Report: 2026-09-02"
    assert email.smtp_host == "smtp.example.edu"
    assert email.smtp_port == 465
    assert email.smtp_no_ssl is False


def test_every_recipient_reaches_the_email_settings() -> None:
    """Each entry is carried across as typed; app.reports parses and renders it."""
    entries = ["a@example.edu, b@example.edu", "Ops Team <ops@example.edu>"]
    email = _targets(_output_args(send_email=True, recipient_email=entries)).email
    assert email is not None
    assert email.recipients == tuple(entries)


def test_a_one_day_window_is_named_by_its_single_date() -> None:
    """The window is half-open, so it covers only the day it opens on."""
    email = _targets(_output_args(send_email=True), ONE_DAY).email
    assert email is not None
    assert email.subject == "JupyterHub Activity Report: 2026-09-02"


def test_a_longer_window_is_named_by_the_days_it_covers() -> None:
    """The last day is the one the window ran through, not the one it stops at."""
    email = _targets(_output_args(send_email=True), ONE_WEEK).email
    assert email is not None
    assert email.subject == "JupyterHub Activity Report: 2026-08-27 to 2026-09-02"


def test_an_explicit_subject_is_sent_exactly_as_given() -> None:
    """Even one that reads like a generated default, and even an empty one."""
    for given in ["Weekly numbers", "JupyterHub Activity Report", ""]:
        email = _targets(_output_args(send_email=True, subject=given), ONE_WEEK).email
        assert email is not None
        assert email.subject == given


#
# compute_report_time_range
#


def _window_args(
    *,
    duration: str | None = None,
    report_start: str | None = None,
    report_end: str | None = None,
    time: str | None = None,
    whole_days: bool = False,
) -> argparse.Namespace:
    """Build the namespace :func:`compute_report_time_range` reads."""
    return argparse.Namespace(
        timezone="America/Chicago",
        duration=duration,
        report_start=report_start,
        report_end=report_end,
        time=time,
        whole_days=whole_days,
    )


def test_an_explicit_range_is_returned_in_the_requested_zone() -> None:
    """Both bounds are read in --timezone, so the window is unambiguous."""
    start, end = compute_report_time_range(
        _window_args(report_start="2026-07-01 00:00", report_end="2026-07-08 00:00")
    )
    assert start == datetime(2026, 7, 1, tzinfo=CHICAGO)
    assert end == datetime(2026, 7, 8, tzinfo=CHICAGO)


def test_an_unparsable_range_is_a_data_shape_error() -> None:
    """Validation normally rejects this before the command runs."""
    with pytest.raises(DataShapeError):
        compute_report_time_range(
            _window_args(report_start="not a datetime", report_end="2026-07-08 00:00")
        )


def test_a_duration_window_ends_now(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without --time the window ends at the moment of the run."""
    now = datetime(2026, 7, 8, 14, 30, tzinfo=CHICAGO)
    monkeypatch.setattr(time_utils, "datetime", FrozenClock(now))

    start, end = compute_report_time_range(_window_args(duration="7 days"))

    assert end == now
    assert end - start == timedelta(days=7)


def test_a_duration_window_can_be_anchored_to_a_wall_clock_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--time steps back to the most recent occurrence of that time."""
    monkeypatch.setattr(
        time_utils, "datetime", FrozenClock(datetime(2026, 7, 8, 14, 30, tzinfo=CHICAGO))
    )

    start, end = compute_report_time_range(_window_args(duration="1d", time="09:00"))

    assert end == datetime(2026, 7, 8, 9, 0, tzinfo=CHICAGO)
    assert start == datetime(2026, 7, 7, 9, 0, tzinfo=CHICAGO)


def test_an_unparsable_duration_is_a_data_shape_error() -> None:
    """Validation normally rejects this before the command runs."""
    with pytest.raises(DataShapeError):
        compute_report_time_range(_window_args(duration="7 fortnights"))


@pytest.mark.parametrize("time", [None, "09:00", "14:30", "23:59"])
def test_a_duration_window_never_ends_in_the_future(
    time: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Why duration mode needs no future-end check of its own.

    Without --time the window ends at the current moment,
    and with one it steps back a day rather than reaching forwards,
    so the anchor cannot outrun the run.
    """
    now = datetime(2026, 7, 8, 14, 30, tzinfo=CHICAGO)
    monkeypatch.setattr(time_utils, "datetime", FrozenClock(now))

    _, end = compute_report_time_range(_window_args(duration="1d", time=time))

    assert end <= now


def test_a_whole_days_range_covers_whole_local_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The window the get-new-users epilog advertises, resolved.

    Without the flag these bounds resolve to 17:48 on each day,
    leaving a partial day at either end.
    """
    monkeypatch.setattr(
        time_utils, "datetime", FrozenClock(datetime(2026, 9, 3, 17, 48, tzinfo=CHICAGO))
    )

    start, end = compute_report_time_range(
        _window_args(report_start="2 weeks ago", report_end="yesterday", whole_days=True)
    )

    assert start == datetime(2026, 8, 20, tzinfo=CHICAGO)
    assert end == datetime(2026, 9, 3, tzinfo=CHICAGO)
    assert end - start == timedelta(days=14)


def test_a_range_without_whole_days_keeps_the_bounds_as_written(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The flag is opt-in, so an existing command line resolves as before."""
    monkeypatch.setattr(
        time_utils, "datetime", FrozenClock(datetime(2026, 9, 3, 17, 48, tzinfo=CHICAGO))
    )

    start, end = compute_report_time_range(
        _window_args(report_start="2 weeks ago", report_end="yesterday")
    )

    assert start == datetime(2026, 8, 20, 17, 48, tzinfo=CHICAGO)
    assert end == datetime(2026, 9, 2, 17, 48, tzinfo=CHICAGO)


#
# The echoed reporting window
#


def test_the_resolved_window_is_echoed_to_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A misparse is visible before the run reaches the network.

    stderr keeps stdout clear for whoever is piping the report itself.
    """
    compute_report_time_range(
        _window_args(report_start="2026-01-03 00:00", report_end="2026-09-03 17:48")
    )

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "Reporting window: 2026-01-03 00:00 to 2026-09-03 17:48"
        " (America/Chicago, 244 days)\n"
    )


def test_a_duration_window_is_echoed_too(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Both window modes announce themselves, in the report's own zone."""
    monkeypatch.setattr(
        time_utils, "datetime", FrozenClock(datetime(2026, 7, 8, 14, 30, tzinfo=CHICAGO))
    )

    compute_report_time_range(_window_args(duration="12h"))

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "Reporting window: 2026-07-08 02:30 to 2026-07-08 14:30"
        " (America/Chicago, 12:00 hours)\n"
    )


def test_the_echo_shows_the_snapped_window(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The line names the window reported on, not the bounds as written."""
    monkeypatch.setattr(
        time_utils, "datetime", FrozenClock(datetime(2026, 9, 3, 17, 48, tzinfo=CHICAGO))
    )

    compute_report_time_range(
        _window_args(report_start="2 weeks ago", report_end="yesterday", whole_days=True)
    )

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "Reporting window: 2026-08-20 00:00 to 2026-09-03 00:00" " (America/Chicago, 14 days)\n"
    )


@pytest.mark.parametrize(
    ("report_end", "expected"),
    [
        ("2026-07-01 00:45", "0:45 hours"),
        ("2026-07-01 12:00", "12:00 hours"),
        ("2026-07-02 00:00", "1 day"),
        ("2026-07-08 00:00", "7 days"),
    ],
)
def test_the_echoed_length_reads_in_days_or_in_hours(
    report_end: str,
    expected: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Days would report a sub-day window as ``0 days``, so it reads H:MM."""
    compute_report_time_range(
        _window_args(report_start="2026-07-01 00:00", report_end=report_end)
    )
    assert capsys.readouterr().err.endswith(f"(America/Chicago, {expected})\n")


def test_the_echoed_length_is_elapsed_time_across_dst(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A half day that loses an hour to spring-forward is eleven hours long."""
    compute_report_time_range(
        _window_args(report_start="2026-03-08 00:00", report_end="2026-03-08 12:00")
    )
    assert capsys.readouterr().err.endswith("(America/Chicago, 11:00 hours)\n")


def test_the_echo_names_the_reports_own_zone(capsys: pytest.CaptureFixture[str]) -> None:
    """--timezone decides the bounds, so it labels them too."""
    args = _window_args(report_start="2026-07-01 00:00", report_end="2026-07-08 00:00")
    args.timezone = "+04:00"

    compute_report_time_range(args)

    assert capsys.readouterr().err == (
        "Reporting window: 2026-07-01 00:00 to 2026-07-08 00:00 (+04:00, 7 days)\n"
    )


#
# get_strftime_fmt
#


@pytest.mark.parametrize(
    ("choice", "expected"),
    [("date", DATE_FMT), ("datetime", DATETIME_FMT)],
)
def test_the_date_format_choice_selects_a_strftime_string(
    choice: str,
    expected: str,
) -> None:
    """The two --date-format choices are the only ones the parser allows."""
    assert get_strftime_fmt(argparse.Namespace(date_format=choice)) == expected
