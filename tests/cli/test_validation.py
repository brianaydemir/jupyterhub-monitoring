"""Tests for :mod:`app.cli.validation`.

Every helper under test reports through ``parser.error``,
which prints usage and raises ``SystemExit(2)``.
Each case therefore asserts on the exit status and on the message
a user would see, since those messages are the module's entire output.
"""

import argparse
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from app.cli import validation
from app.cli.arguments import add_email_argument_group, add_es_argument_group
from app.cli.validation import (
    positive_int_or_error,
    reject_es_flags_without_endpoint,
    validate_email_arguments,
    validate_es_arguments,
    validate_jupyterhub_arguments,
    validate_query_arguments,
)
from app.core import time_utils
from tests.helpers.clock import FrozenClock

Validator = Callable[[argparse.Namespace, argparse.ArgumentParser], None]

# The moment the future-end cases are measured against.
# It is in the zone ``_query_args`` defaults to,
# so an offset from it can be written as a bare datetime
# and read back in the same zone.
NOW = datetime(2026, 7, 8, 14, 30, tzinfo=ZoneInfo("America/Chicago"))


@pytest.fixture(autouse=True)
def without_ambient_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the host's own API keys from deciding a case's outcome."""
    monkeypatch.delenv("JUPYTERHUB_API_KEY", raising=False)
    monkeypatch.delenv("ELASTICSEARCH_API_KEY", raising=False)


def _reject(
    validate: Validator,
    args: argparse.Namespace,
    capsys: pytest.CaptureFixture[str],
) -> str:
    """Run *validate*, assert it exited 2, and return what it printed."""
    parser = argparse.ArgumentParser(prog="cmd")
    with pytest.raises(SystemExit) as exit_info:
        validate(args, parser)
    assert exit_info.value.code == 2
    return capsys.readouterr().err


def _accept(validate: Validator, args: argparse.Namespace) -> None:
    """Run *validate* and assert that it reported nothing."""
    validate(args, argparse.ArgumentParser(prog="cmd"))


#
# positive_int_or_error
#


@pytest.mark.parametrize("value", [None, 1, 100])
def test_an_absent_or_positive_int_is_accepted(value: int | None) -> None:
    """The check is optional: an unset flag is not an error."""
    positive_int_or_error(value, parser=argparse.ArgumentParser(prog="cmd"), flag="--limit")


@pytest.mark.parametrize("value", [0, -1])
def test_a_non_positive_int_names_its_flag(
    value: int,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The flag is named so the message points at the argument to fix."""
    parser = argparse.ArgumentParser(prog="cmd")
    with pytest.raises(SystemExit) as exit_info:
        positive_int_or_error(value, parser=parser, flag="--limit")
    assert exit_info.value.code == 2
    assert "--limit must be a positive integer" in capsys.readouterr().err


#
# validate_jupyterhub_arguments
#


def _jupyterhub_args(
    *,
    api_key: Path | None = None,
    ca_cert: Path | None = None,
) -> argparse.Namespace:
    """Build the namespace :func:`validate_jupyterhub_arguments` reads."""
    return argparse.Namespace(jupyterhub_api_key=api_key, jupyterhub_ca_cert=ca_cert)


def test_a_missing_jupyterhub_ca_cert_is_rejected(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A path that does not exist fails now rather than at connect time."""
    missing = tmp_path / "absent.pem"
    message = _reject(
        validate_jupyterhub_arguments,
        _jupyterhub_args(ca_cert=missing),
        capsys,
    )
    assert f"CA certificate file not found: {missing}" in message


def test_a_missing_jupyterhub_api_key_file_is_rejected(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An explicit key file must exist, whatever the environment holds."""
    missing = tmp_path / "absent.txt"
    message = _reject(
        validate_jupyterhub_arguments,
        _jupyterhub_args(api_key=missing),
        capsys,
    )
    assert f"API key file not found: {missing}" in message


def test_a_jupyterhub_key_is_required_from_some_source(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """With neither flag nor environment variable, there is no credential."""
    message = _reject(validate_jupyterhub_arguments, _jupyterhub_args(), capsys)
    assert (
        "--jupyterhub-api-key or the JUPYTERHUB_API_KEY environment variable is required"
        in message
    )


def test_the_jupyterhub_environment_variable_supplies_the_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The environment stands in for the flag, as the help text promises."""
    monkeypatch.setenv("JUPYTERHUB_API_KEY", "secret")
    _accept(validate_jupyterhub_arguments, _jupyterhub_args())


def test_existing_jupyterhub_files_are_accepted(tmp_path: Path) -> None:
    """Both paths are checked for existence only, not for content."""
    api_key = tmp_path / "key.txt"
    api_key.write_text("secret", encoding="utf-8")
    ca_cert = tmp_path / "ca.pem"
    ca_cert.write_text("", encoding="utf-8")
    _accept(validate_jupyterhub_arguments, _jupyterhub_args(api_key=api_key, ca_cert=ca_cert))


#
# validate_es_arguments
#


def _es_args(
    *,
    api_key: Path | None = None,
    ca_cert: Path | None = None,
    username: str | None = None,
) -> argparse.Namespace:
    """Build the namespace :func:`validate_es_arguments` reads."""
    return argparse.Namespace(es_api_key=api_key, es_ca_cert=ca_cert, es_username=username)


def test_a_missing_es_ca_cert_is_rejected(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The CA certificate is checked before any credential is."""
    missing = tmp_path / "absent.pem"
    message = _reject(
        validate_es_arguments,
        _es_args(ca_cert=missing, username="elastic"),
        capsys,
    )
    assert f"CA certificate file not found: {missing}" in message


def test_an_es_key_file_and_a_username_are_mutually_exclusive(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Two explicit credentials leave no way to choose between them."""
    api_key = tmp_path / "key.txt"
    api_key.write_text("secret", encoding="utf-8")
    message = _reject(
        validate_es_arguments,
        _es_args(api_key=api_key, username="elastic"),
        capsys,
    )
    assert "--es-api-key and --es-username are mutually exclusive" in message


def test_an_es_credential_is_required_from_some_source(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """All three sources are named, since any one of them would do."""
    message = _reject(validate_es_arguments, _es_args(), capsys)
    assert "one of --es-api-key, ELASTICSEARCH_API_KEY, or --es-username is required" in message


def test_a_missing_es_api_key_file_is_rejected(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A named key file must exist; existence is all that is checked."""
    missing = tmp_path / "absent.txt"
    message = _reject(validate_es_arguments, _es_args(api_key=missing), capsys)
    assert f"API key file not found: {missing}" in message


@pytest.mark.parametrize("username", [None, "elastic"])
def test_the_es_environment_variable_supplies_the_credential(
    username: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The environment satisfies the credential requirement on its own.

    Only an explicitly passed --es-api-key conflicts with --es-username,
    so a container with the secret injected can still be run interactively.
    """
    monkeypatch.setenv("ELASTICSEARCH_API_KEY", "secret")
    _accept(validate_es_arguments, _es_args(username=username))


def test_a_username_alone_supplies_the_credential() -> None:
    """The password is prompted for later, so the username is enough."""
    _accept(validate_es_arguments, _es_args(username="elastic"))


#
# reject_es_flags_without_endpoint
#


@pytest.mark.parametrize(
    "argv",
    [
        ["--es-index", "servers"],
        ["--es-ca-cert", "ca.pem"],
        ["--es-api-key", "es-api-key.txt"],
        ["--es-username", "elastic"],
    ],
)
def test_an_es_flag_without_an_endpoint_is_rejected(
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """With nothing to connect to, the flag would do nothing."""
    parser = argparse.ArgumentParser(prog="cmd")
    add_es_argument_group(parser, required=False)
    message = _reject(reject_es_flags_without_endpoint, parser.parse_args(argv), capsys)
    assert f"{argv[0]} requires --es-endpoint" in message


#
# validate_query_arguments
#


def _query_args(
    *,
    duration: str | None = None,
    report_start: str | None = None,
    report_end: str | None = None,
    time: str | None = None,
    timezone: str = "America/Chicago",
    whole_days: bool = False,
) -> argparse.Namespace:
    """Build the namespace :func:`validate_query_arguments` reads."""
    return argparse.Namespace(
        duration=duration,
        report_start=report_start,
        report_end=report_end,
        time=time,
        timezone=timezone,
        whole_days=whole_days,
    )


def _freeze(monkeypatch: pytest.MonkeyPatch) -> None:
    """Freeze both clocks a window is measured against, at :data:`NOW`.

    The future-end check reads this module's,
    and the snap ``--whole-days`` applies reads :mod:`app.core.time_utils`'s,
    so a case that exercises both has to hold the two together.
    """
    monkeypatch.setattr(validation, "datetime", FrozenClock(NOW))
    monkeypatch.setattr(time_utils, "datetime", FrozenClock(NOW))


def test_an_unrecognized_timezone_is_rejected(capsys: pytest.CaptureFixture[str]) -> None:
    """The zone is parsed first, because the window is read in it."""
    message = _reject(
        validate_query_arguments,
        _query_args(duration="7 days", timezone="Mars/Olympus"),
        capsys,
    )
    assert "Unrecognized timezone" in message


def test_a_duration_and_a_range_are_mutually_exclusive(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The two window modes would otherwise silently disagree."""
    message = _reject(
        validate_query_arguments,
        _query_args(duration="7 days", report_start="2026-07-01 00:00"),
        capsys,
    )
    assert "--duration is mutually exclusive with --report-start / --report-end" in message


def test_a_window_mode_is_required(capsys: pytest.CaptureFixture[str]) -> None:
    """Neither mode means there is no window to report on."""
    message = _reject(validate_query_arguments, _query_args(), capsys)
    assert "one of --duration or --report-start / --report-end is required" in message


def test_an_unparsable_duration_is_rejected(capsys: pytest.CaptureFixture[str]) -> None:
    """The offending value is quoted back so the typo is visible."""
    message = _reject(validate_query_arguments, _query_args(duration="7 fortnights"), capsys)
    assert "Invalid duration format: '7 fortnights'" in message


@pytest.mark.parametrize("duration", ["0s", "-5m"])
def test_a_non_positive_duration_is_rejected(
    duration: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A zero or backwards window parses, so the sign is checked here."""
    message = _reject(validate_query_arguments, _query_args(duration=duration), capsys)
    assert f"--duration must be positive, not {duration!r}" in message


def test_a_positive_duration_is_accepted() -> None:
    """The ordinary duration-mode window passes every check."""
    _accept(validate_query_arguments, _query_args(duration="7 days"))


def test_a_half_specified_range_is_rejected(capsys: pytest.CaptureFixture[str]) -> None:
    """One bound alone does not describe a window."""
    message = _reject(
        validate_query_arguments,
        _query_args(report_start="2026-07-01 00:00"),
        capsys,
    )
    assert "--report-start and --report-end must be used together" in message


def test_a_range_cannot_be_anchored_with_time(capsys: pytest.CaptureFixture[str]) -> None:
    """--time anchors a duration; an explicit range already has an end."""
    message = _reject(
        validate_query_arguments,
        _query_args(
            report_start="2026-07-01 00:00",
            report_end="2026-07-08 00:00",
            time="09:00",
        ),
        capsys,
    )
    assert "--time cannot be combined with --report-start / --report-end" in message


def test_an_unparsable_report_start_is_rejected(capsys: pytest.CaptureFixture[str]) -> None:
    """Each bound is reported by its own flag name."""
    message = _reject(
        validate_query_arguments,
        _query_args(report_start="not a datetime", report_end="2026-07-08 00:00"),
        capsys,
    )
    assert "Invalid --report-start datetime: 'not a datetime'" in message


def test_an_unparsable_report_end_is_rejected(capsys: pytest.CaptureFixture[str]) -> None:
    """A good start does not excuse a bad end."""
    message = _reject(
        validate_query_arguments,
        _query_args(report_start="2026-07-01 00:00", report_end="not a datetime"),
        capsys,
    )
    assert "Invalid --report-end datetime: 'not a datetime'" in message


@pytest.mark.parametrize("end", ["2026-07-01 00:00", "2026-06-24 00:00"])
def test_a_range_must_run_forwards(end: str, capsys: pytest.CaptureFixture[str]) -> None:
    """An empty or backwards window would report on nothing."""
    message = _reject(
        validate_query_arguments,
        _query_args(report_start="2026-07-01 00:00", report_end=end),
        capsys,
    )
    assert "--report-start must be before --report-end" in message


def test_a_forward_range_is_accepted() -> None:
    """The ordinary range-mode window passes every check."""
    _accept(
        validate_query_arguments,
        _query_args(report_start="2026-07-01 00:00", report_end="2026-07-08 00:00"),
    )


def _end_offset_from_now(monkeypatch: pytest.MonkeyPatch, offset: timedelta) -> str:
    """Freeze the clock at :data:`NOW` and spell *offset* from it as an end.

    The result is an absolute datetime,
    so only the reference the check reads is frozen;
    parsing the bound itself consults no clock.
    """
    monkeypatch.setattr(validation, "datetime", FrozenClock(NOW))
    return (NOW + offset).strftime("%Y-%m-%d %H:%M:%S")


@pytest.mark.parametrize("end", ["tomorrow", "next week"])
def test_a_future_report_end_is_rejected(
    end: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """No data can exist for a window reaching past the present.

    Both values parse and both run forwards from the start,
    so this check is the only thing
    standing between the typo and a wrong report.
    """
    message = _reject(
        validate_query_arguments,
        _query_args(report_start="2 weeks ago", report_end=end),
        capsys,
    )
    assert "--report-end resolves to" in message
    assert "which is in the future" in message


def test_a_report_end_of_now_is_accepted() -> None:
    """The documented ``now`` must pass reliably, not most of the time.

    It is resolved
    here and again in :func:`app.cli.factories.compute_report_time_range`,
    and the reference it is compared against is read later still,
    so no two of those answers are the same instant.
    """
    _accept(
        validate_query_arguments,
        _query_args(report_start="1 day ago", report_end="now"),
    )


def test_an_end_within_the_tolerance_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    """The tolerance is what keeps ``now`` from failing on its own slop."""
    end = _end_offset_from_now(monkeypatch, timedelta(seconds=30))
    _accept(
        validate_query_arguments,
        _query_args(report_start="2026-07-01 00:00", report_end=end),
    )


def test_an_end_past_the_tolerance_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Two minutes out is a wrong window rather than resolution slop."""
    end = _end_offset_from_now(monkeypatch, timedelta(minutes=2))
    message = _reject(
        validate_query_arguments,
        _query_args(report_start="2026-07-01 00:00", report_end=end),
        capsys,
    )
    assert "--report-end resolves to 2026-07-08 14:32, which is in the future" in message


@pytest.mark.parametrize("time", ["9am", "0900", "9", "12:345"])
def test_a_malformed_time_is_rejected(
    time: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Only HH:MM is accepted, so the anchor cannot be misread."""
    message = _reject(
        validate_query_arguments,
        _query_args(duration="7 days", time=time),
        capsys,
    )
    assert "--time must be in HH:MM format" in message


@pytest.mark.parametrize("time", ["24:00", "99:00", "12:60", "12:99"])
def test_an_out_of_range_time_is_rejected(
    time: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A well-formed value still has to name a real wall-clock time."""
    message = _reject(
        validate_query_arguments,
        _query_args(duration="7 days", time=time),
        capsys,
    )
    assert "--time must be a valid time (HH: 0-23, MM: 0-59)" in message


@pytest.mark.parametrize("time", ["9:30", "09:30", "00:00", "23:59"])
def test_a_valid_time_anchor_is_accepted(time: str) -> None:
    """One- and two-digit hours are both allowed."""
    _accept(validate_query_arguments, _query_args(duration="7 days", time=time))


def test_whole_days_cannot_be_combined_with_a_duration(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--duration anchors its own end with --time, so the flag would do nothing."""
    message = _reject(
        validate_query_arguments,
        _query_args(duration="7 days", whole_days=True),
        capsys,
    )
    assert "--whole-days cannot be combined with --duration" in message


def test_a_whole_days_window_ending_today_is_not_a_future_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The snap stops at the last midnight, so it cannot outrun the check.

    Widening this end to tomorrow's midnight
    would put the window hours into the future
    and fail a legitimate command line.
    """
    _freeze(monkeypatch)
    _accept(
        validate_query_arguments,
        _query_args(report_start="2026-07-01 00:00", report_end="now", whole_days=True),
    )


def test_whole_days_does_not_rescue_a_future_report_end(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Snapping a typo back to midnight still leaves it ahead of the present."""
    message = _reject(
        validate_query_arguments,
        _query_args(report_start="2 weeks ago", report_end="tomorrow", whole_days=True),
        capsys,
    )
    assert "--report-end resolves to" in message
    assert "which is in the future" in message


def test_a_whole_days_window_with_no_finished_day_is_rejected(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Both bounds land on today's midnight, which would report on nothing."""
    _freeze(monkeypatch)
    message = _reject(
        validate_query_arguments,
        _query_args(
            report_start="2026-07-08 09:00",
            report_end="2026-07-08 14:00",
            whole_days=True,
        ),
        capsys,
    )
    assert "--whole-days leaves no whole day between --report-start and --report-end" in message


def test_a_backwards_range_is_reported_as_such_even_with_whole_days(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The bounds the user wrote are checked before the snap touches them."""
    _freeze(monkeypatch)
    message = _reject(
        validate_query_arguments,
        _query_args(
            report_start="2026-07-08 00:00",
            report_end="2026-07-01 00:00",
            whole_days=True,
        ),
        capsys,
    )
    assert "--report-start must be before --report-end" in message


#
# validate_email_arguments
#


def _email_args(
    *,
    send_email: bool = True,
    sender_email: str | None = "reports@example.edu",
    recipient_email: Sequence[str] | None = ("ops@example.edu",),
    smtp_host: str | None = "smtp.example.edu",
    smtp_port: int | None = 465,
) -> argparse.Namespace:
    """Build the namespace :func:`validate_email_arguments` reads.

    ``--recipient-email`` collects a list,
    so *recipient_email* is a sequence here;
    ``None`` is the flag having been left out.
    """
    return argparse.Namespace(
        send_email=send_email,
        sender_email=sender_email,
        recipient_email=recipient_email,
        sender_name=None,
        subject=None,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_no_ssl=False,
    )


def test_email_arguments_are_not_required_without_send_email() -> None:
    """The flags are optional, so they are gated on --send-email."""
    _accept(
        validate_email_arguments,
        _email_args(
            send_email=False,
            sender_email=None,
            recipient_email=None,
            smtp_host=None,
            smtp_port=None,
        ),
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["--sender-email", "reports@example.edu"],
        ["--recipient-email", "ops@example.edu"],
        ["--sender-name", "Reports"],
        ["--subject", "Weekly report"],
        ["--smtp-host", "smtp.example.edu"],
        ["--smtp-port", "465"],
        ["--smtp-no-ssl"],
    ],
)
def test_an_email_argument_without_send_email_is_rejected(
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Without --send-email, nothing would read the flag."""
    parser = argparse.ArgumentParser(prog="cmd")
    add_email_argument_group(parser, subject_prefix="Report")
    message = _reject(validate_email_arguments, parser.parse_args(argv), capsys)
    assert f"{argv[0]} requires --send-email" in message


@pytest.mark.parametrize(
    ("flag", "attr"),
    [
        ("--sender-email", "sender_email"),
        ("--recipient-email", "recipient_email"),
        ("--smtp-host", "smtp_host"),
        ("--smtp-port", "smtp_port"),
    ],
)
def test_each_email_argument_is_required_with_send_email(
    flag: str,
    attr: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Every one of the four is reported by its own flag name."""
    args = _email_args()
    setattr(args, attr, None)
    message = _reject(validate_email_arguments, args, capsys)
    assert f"{flag} is required when --send-email is set" in message


def test_a_complete_email_configuration_is_accepted() -> None:
    """The optional --sender-name and --smtp-no-ssl are not required."""
    _accept(validate_email_arguments, _email_args())
