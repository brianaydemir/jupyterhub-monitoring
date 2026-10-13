"""Tests for :mod:`app.core.time_utils`."""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from app.core import time_utils
from app.core.time_utils import (
    compute_time_range,
    elapsed,
    format_timezone,
    get_now_ms,
    last_instant,
    parse_datetime,
    parse_duration,
    parse_iso_utc,
    parse_timezone,
    snap_to_whole_days,
)
from tests.helpers.clock import FrozenClock

CHICAGO = ZoneInfo("America/Chicago")


@pytest.mark.parametrize(
    ("text", "expected_seconds"),
    [("+04:00", 4 * 3600), ("-05:30", -(5 * 3600 + 1800)), ("+00:00", 0)],
)
def test_parse_timezone_accepts_fixed_offsets(text: str, expected_seconds: int) -> None:
    """A ``+/-HH:MM`` string becomes a fixed-offset zone."""
    offset = parse_timezone(text).utcoffset(None)
    assert offset is not None
    assert offset.total_seconds() == expected_seconds


@pytest.mark.parametrize("text", ["Not/AZone", "+24:00", "+04:60", "04:00", "", "5pm"])
def test_parse_timezone_rejects_bad_values(text: str) -> None:
    """Anything else raises, naming the offending value."""
    with pytest.raises(ValueError, match="Unrecognized timezone"):
        parse_timezone(text)


@pytest.mark.parametrize(
    "text", ["America/Chicago", "MST", "UTC", "+04:00", "-05:30", "+00:00"]
)
def test_format_timezone_round_trips_every_accepted_form(text: str) -> None:
    """A parsed zone still knows the name it was built from."""
    assert format_timezone(parse_timezone(text)) == text


def test_compute_time_range_ends_now_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without an anchor the window ends at the current instant."""
    now = datetime(2026, 7, 4, 9, 30, tzinfo=CHICAGO)
    start, end = _range_at(monkeypatch, now, timedelta(hours=6), None)
    assert end == now
    assert start == datetime(2026, 7, 4, 3, 30, tzinfo=CHICAGO)


def test_compute_time_range_anchors_to_a_past_wall_clock_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An anchor already past today is used as given."""
    now = datetime(2026, 7, 4, 9, 30, tzinfo=CHICAGO)
    _, end = _range_at(monkeypatch, now, timedelta(days=1), "06:00")
    assert end == datetime(2026, 7, 4, 6, 0, tzinfo=CHICAGO)


def test_compute_time_range_anchor_steps_back_a_day_when_future(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An anchor still ahead today resolves to yesterday's occurrence."""
    now = datetime(2026, 7, 4, 9, 30, tzinfo=CHICAGO)
    start, end = _range_at(monkeypatch, now, timedelta(days=1), "22:00")
    assert end == datetime(2026, 7, 3, 22, 0, tzinfo=CHICAGO)
    assert start == datetime(2026, 7, 2, 22, 0, tzinfo=CHICAGO)


@pytest.mark.parametrize(
    "anchor",
    [
        # Spring forward: wall-clock subtraction would span 23 hours.
        datetime(2026, 3, 8, 12, 0, tzinfo=CHICAGO),
        # Fall back: wall-clock subtraction would span 25 hours.
        datetime(2026, 11, 1, 12, 0, tzinfo=CHICAGO),
        # A day with no transition, as a control.
        datetime(2026, 7, 4, 12, 0, tzinfo=CHICAGO),
    ],
)
def test_compute_time_range_spans_the_requested_duration_across_dst(
    anchor: datetime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A day-long window is a real day, whatever the local offset does."""
    start, end = _range_at(monkeypatch, anchor, timedelta(days=1), None)
    assert end.timestamp() - start.timestamp() == timedelta(days=1).total_seconds()


@pytest.mark.parametrize(
    ("day", "hours"),
    [
        pytest.param(datetime(2026, 3, 8, tzinfo=CHICAGO), 23, id="spring-forward"),
        pytest.param(datetime(2026, 11, 1, tzinfo=CHICAGO), 25, id="fall-back"),
        pytest.param(datetime(2026, 7, 4, tzinfo=CHICAGO), 24, id="no-transition"),
    ],
)
def test_elapsed_measures_real_time_across_dst(day: datetime, hours: int) -> None:
    """Subtracting the bounds directly would read every one of these as a day.

    Two datetimes sharing a ``tzinfo`` subtract by wall clock,
    which is what the report's window length must not do.
    """
    assert elapsed(day, day + timedelta(days=1)) == timedelta(hours=hours)


def test_compute_time_range_rejects_a_malformed_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-``HH:MM`` anchor is a programming error here, not a silent skip."""
    with pytest.raises(ValueError):
        _range_at(monkeypatch, datetime(2026, 7, 4, tzinfo=CHICAGO), timedelta(days=1), "noon")


def _range_at(
    monkeypatch: pytest.MonkeyPatch,
    now: datetime,
    duration: timedelta,
    time_str: str | None,
) -> tuple[datetime, datetime]:
    """Evaluate :func:`compute_time_range` with the clock frozen at *now*."""
    monkeypatch.setattr(time_utils, "datetime", FrozenClock(now))
    return compute_time_range(duration, time_str, CHICAGO)


def _snap_at(
    monkeypatch: pytest.MonkeyPatch,
    now: datetime,
    start: datetime,
    end: datetime,
) -> tuple[datetime, datetime]:
    """Evaluate :func:`snap_to_whole_days` with the clock frozen at *now*."""
    monkeypatch.setattr(time_utils, "datetime", FrozenClock(now))
    return snap_to_whole_days(start, end, CHICAGO)


def test_snap_widens_both_bounds_to_the_days_they_fall_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The documented two-week window, with its ragged edges squared off."""
    start, end = _snap_at(
        monkeypatch,
        datetime(2026, 9, 3, 17, 48, tzinfo=CHICAGO),
        datetime(2026, 8, 20, 17, 48, tzinfo=CHICAGO),
        datetime(2026, 9, 2, 17, 48, tzinfo=CHICAGO),
    )
    assert start == datetime(2026, 8, 20, tzinfo=CHICAGO)
    assert end == datetime(2026, 9, 3, tzinfo=CHICAGO)


def test_snap_covers_one_day_as_a_half_open_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A single day runs from its own midnight to the next one, exclusive."""
    start, end = _snap_at(
        monkeypatch,
        datetime(2026, 9, 3, 17, 48, tzinfo=CHICAGO),
        datetime(2026, 9, 2, 9, 15, tzinfo=CHICAGO),
        datetime(2026, 9, 2, 16, 45, tzinfo=CHICAGO),
    )
    assert (start, end) == (
        datetime(2026, 9, 2, tzinfo=CHICAGO),
        datetime(2026, 9, 3, tzinfo=CHICAGO),
    )


def test_snap_leaves_bounds_already_on_midnight_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Snapping an aligned window is a no-op, so the snap is idempotent."""
    aligned = (
        datetime(2026, 7, 1, tzinfo=CHICAGO),
        datetime(2026, 7, 8, tzinfo=CHICAGO),
    )
    now = datetime(2026, 9, 3, 17, 48, tzinfo=CHICAGO)
    assert _snap_at(monkeypatch, now, *aligned) == aligned


def test_snap_stops_at_the_last_midnight_that_has_passed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A window ending now closes with the last day that finished.

    Reaching forwards to tomorrow's midnight would claim a day
    the run is still in the middle of,
    and would trip the future-end check in :mod:`app.cli.validation`.
    """
    now = datetime(2026, 9, 3, 17, 48, tzinfo=CHICAGO)
    _, end = _snap_at(monkeypatch, now, datetime(2026, 9, 1, 8, 0, tzinfo=CHICAGO), now)
    assert end == datetime(2026, 9, 3, tzinfo=CHICAGO)
    assert end < now


def test_snap_leaves_an_end_that_is_genuinely_future_in_the_future(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """That fallback must not launder a typo into an acceptable window."""
    now = datetime(2026, 9, 3, 17, 48, tzinfo=CHICAGO)
    _, end = _snap_at(
        monkeypatch,
        now,
        datetime(2026, 9, 1, 8, 0, tzinfo=CHICAGO),
        datetime(2026, 9, 4, 17, 48, tzinfo=CHICAGO),
    )
    assert end == datetime(2026, 9, 4, tzinfo=CHICAGO)
    assert end > now


@pytest.mark.parametrize(
    ("day", "hours"),
    [
        pytest.param(datetime(2026, 3, 8, 12, tzinfo=CHICAGO), 23, id="spring-forward"),
        pytest.param(datetime(2026, 11, 1, 12, tzinfo=CHICAGO), 25, id="fall-back"),
        pytest.param(datetime(2026, 7, 4, 12, tzinfo=CHICAGO), 24, id="no-transition"),
    ],
)
def test_snap_finds_midnight_by_calendar_date_across_dst(
    day: datetime,
    hours: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transition day is 23 or 25 hours long, and still begins at 00:00."""
    start, end = _snap_at(monkeypatch, datetime(2026, 12, 31, 12, tzinfo=CHICAGO), day, day)
    assert (start.hour, start.minute) == (0, 0)
    assert (end.hour, end.minute) == (0, 0)
    assert end.timestamp() - start.timestamp() == hours * 3600


def test_last_instant_names_a_midnight_end_by_the_day_before() -> None:
    """A window closing at midnight is named by the day it reported on."""
    assert last_instant(datetime(2026, 9, 3, tzinfo=CHICAGO)).date() == date(2026, 9, 2)


def test_snap_reads_the_day_in_the_given_zone(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bound carrying another offset is snapped where the report reads it."""
    start, end = _snap_at(
        monkeypatch,
        datetime(2026, 9, 3, 17, 48, tzinfo=CHICAGO),
        datetime(2026, 9, 1, 2, 0, tzinfo=timezone.utc),
        datetime(2026, 9, 2, 2, 0, tzinfo=timezone.utc),
    )
    assert start == datetime(2026, 8, 31, tzinfo=CHICAGO)
    assert end == datetime(2026, 9, 2, tzinfo=CHICAGO)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("7 days", timedelta(days=7)),
        ("12h", timedelta(hours=12)),
        ("15 min", timedelta(minutes=15)),
        ("30 seconds", timedelta(seconds=30)),
        ("3d 6h 12m", timedelta(days=3, hours=6, minutes=12)),
    ],
)
def test_parse_duration_accepts_documented_forms(text: str, expected: timedelta) -> None:
    """Every duration spelling the help text advertises parses."""
    assert parse_duration(text) == expected


def test_parse_duration_rejects_unparsable_text() -> None:
    """An unparsable duration is reported as *None*, not a guess."""
    assert parse_duration("whenever") is None


@pytest.mark.parametrize(("text", "seconds"), [("0s", 0), ("-1h", -3600)])
def test_parse_duration_returns_non_positive_values(text: str, seconds: int) -> None:
    """Zero and negative durations parse, so callers must check the sign."""
    parsed = parse_duration(text)
    assert parsed is not None
    assert parsed.total_seconds() == seconds


def test_parse_iso_utc_assumes_utc_when_no_offset_is_given() -> None:
    """A naive timestamp is read as UTC, not as the host's local time."""
    assert parse_iso_utc("2026-07-01T10:00:00") == datetime(
        2026, 7, 1, 10, 0, tzinfo=timezone.utc
    )


def test_parse_iso_utc_keeps_an_explicit_offset() -> None:
    """A timestamp carrying an offset is returned unchanged."""
    parsed = parse_iso_utc("2026-07-01T10:00:00+02:00")
    assert parsed is not None
    assert parsed.utcoffset() == timedelta(hours=2)


@pytest.mark.parametrize("value", [None, "", "not-a-date", 12345, {"a": 1}])
def test_parse_iso_utc_rejects_unusable_values(value: object) -> None:
    """Anything that is not a parseable ISO 8601 string yields *None*."""
    assert parse_iso_utc(value) is None


def test_parse_datetime_normalizes_to_the_requested_zone() -> None:
    """An absolute datetime is interpreted in the given zone."""
    parsed = parse_datetime("2026-07-01 15:00", CHICAGO)
    assert parsed == datetime(2026, 7, 1, 15, 0, tzinfo=CHICAGO)


def test_parse_datetime_converts_an_explicit_offset() -> None:
    """A datetime with its own offset is converted, not relabeled."""
    parsed = parse_datetime("2026-07-01 15:00 +00:00", CHICAGO)
    assert parsed is not None
    assert parsed.utcoffset() == timedelta(hours=-5)


def test_parse_datetime_accepts_a_fixed_offset_zone() -> None:
    """The zone forms :func:`parse_timezone` accepts work here too."""
    parsed = parse_datetime("2026-07-01 15:00", parse_timezone("+04:00"))
    assert parsed is not None
    assert parsed.utcoffset() == timedelta(hours=4)


def test_parse_datetime_rejects_unparsable_text() -> None:
    """Unparsable text yields *None* rather than a fabricated date."""
    assert parse_datetime("the summer of love", timezone.utc) is None


def test_get_now_ms_is_a_millisecond_epoch() -> None:
    """The value is milliseconds, consistent with Elasticsearch key times."""
    before = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    now_ms = get_now_ms()
    after = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    assert before - 1 <= now_ms <= after + 1
