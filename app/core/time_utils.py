"""Utilities for parsing times and durations, and computing time ranges."""

import re
from datetime import datetime, timedelta, timezone, tzinfo
from functools import lru_cache
from zoneinfo import ZoneInfo

import dateparser
import pytimeparse2

from app.core.errors import DataShapeError

_OFFSET_RE = re.compile(r"^([+-])(\d{1,2}):(\d{2})$")

# Display formats for report timestamps, shared by the CLI layer
# (which maps ``--date-format`` onto them,
# and passes the datetime one for a command that takes no such option)
# and the report builders.
DATE_FMT = "%Y-%m-%d"
DATETIME_FMT = "%Y-%m-%d %H:%M"

# The step back from a half-open window's end to its last instant.
# ``datetime`` resolves to the microsecond, so this is exact.
_ONE_MICROSECOND = timedelta(microseconds=1)


def parse_timezone(tz_str: str) -> tzinfo:
    """Parse a timezone string into a :class:`datetime.tzinfo` object.

    Accepts IANA timezone names (``"America/Chicago"``), common abbreviations
    that are valid IANA keys (``"MST"``),
    or fixed UTC-offset strings in the form ``"+HH:MM"`` / ``"-HH:MM"``.

    Raises:
        ValueError: If the string cannot be interpreted as a valid timezone.
    """
    # Try IANA name / abbreviation first.
    # An unknown key raises ZoneInfoNotFoundError (a KeyError subclass)
    # and a malformed one raises ValueError,
    # so catch both and let the fixed-offset branch have its turn.
    try:
        return ZoneInfo(tz_str)
    except KeyError, ValueError:
        pass

    # Try fixed UTC offset: +/-HH:MM
    if m := _OFFSET_RE.match(tz_str):
        hours, minutes = int(m.group(2)), int(m.group(3))
        if hours <= 23 and minutes <= 59:
            sign = 1 if m.group(1) == "+" else -1
            offset = timedelta(hours=hours, minutes=minutes) * sign
            return timezone(offset, name=tz_str)

    raise ValueError(f"Unrecognized timezone: {tz_str!r}")


def format_timezone(tz: tzinfo) -> str:
    """Return the display name of a zone, inverting :func:`parse_timezone`.

    Every form :func:`parse_timezone` accepts round-trips:
    a :class:`~zoneinfo.ZoneInfo` renders as the IANA key it was built from,
    and a fixed-offset zone carries its ``+/-HH:MM`` string as its name.
    Callers therefore need not carry the original string
    alongside the parsed zone.
    """
    return str(tz)


def compute_time_range(
    duration_td: timedelta,
    time_str: str | None,
    tz: tzinfo,
) -> tuple[datetime, datetime]:
    """Compute the (start, end) datetime pair for a reporting window.

    *time_str* anchors the end of the window to a wall-clock time,
    so it is resolved in *tz*.
    The duration itself is subtracted in UTC,
    because the window is compared against absolute timestamps:
    wall-clock subtraction would make a window that spans a DST transition
    23 or 25 hours long.

    Raises:
        ValueError: If *time_str* is not in ``HH:MM`` form.
    """
    now = datetime.now(tz)

    if time_str is not None:
        hh, mm = (int(x) for x in time_str.split(":"))
        # Candidate: today's date at HH:MM in the given tz.
        candidate = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        # If that moment is in the future, step back one day.
        if candidate > now:
            candidate -= timedelta(days=1)
        end = candidate
    else:
        end = now

    start = (end.astimezone(timezone.utc) - duration_td).astimezone(tz)
    return start, end


def elapsed(start: datetime, end: datetime) -> timedelta:
    """Return the real time from *start* to *end*.

    Subtracting two datetimes that share a ``tzinfo``
    compares their wall-clock readings,
    so across a daylight-saving transition the difference is an hour off.
    Converting both to UTC first measures elapsed time,
    the unit every duration in a report is summed in.
    """
    return end.astimezone(timezone.utc) - start.astimezone(timezone.utc)


def parse_datetime(text: str, tz: tzinfo) -> datetime | None:
    """Parse a human-readable datetime, normalized to *tz*.

    Accepts relative phrases (``"2 weeks ago"``, ``"yesterday 9am"``)
    and absolute datetimes (``"July 1 2026"``, ``"2026-07-01 15:00"``),
    via :func:`dateparser.parse`.
    A datetime with no explicit zone is interpreted in *tz*;
    one that carries its own offset is converted to *tz*.
    Returns *None* if *text* cannot be parsed.

    The zone is applied here
    rather than through :mod:`dateparser`'s own timezone settings,
    which reject some of the offsets :func:`parse_timezone` accepts.
    """
    parsed = dateparser.parse(
        text,
        settings={
            "PREFER_DATES_FROM": "past",
            "RELATIVE_BASE": datetime.now(tz).replace(tzinfo=None),
        },
    )
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)


def snap_to_whole_days(
    start: datetime,
    end: datetime,
    tz: tzinfo,
) -> tuple[datetime, datetime]:
    """Widen a window so that both of its bounds fall on midnight in *tz*.

    *start* moves back to the midnight that opens its own day,
    and *end* forward to the next midnight,
    so that the half-open window covers in full every day it touches.
    A bound already at midnight stays where it is,
    which leaves an aligned window alone and makes the snap idempotent.

    An *end* whose next midnight has not yet arrived moves back
    to its own midnight instead:
    the window closes with the last day that finished
    rather than reaching into one that has not.
    That is what keeps ``--report-end "now"`` clear of the future-end check
    in :func:`app.cli.validation.validate_query_arguments`;
    an end that is genuinely ahead, such as ``"tomorrow"``,
    still lands in the future and is still rejected there.

    Both midnights are found by calendar date in *tz*,
    not by adding or subtracting 24 hours,
    so a window spanning a daylight-saving transition still begins and ends
    at local midnight.
    """
    end_day = _start_of_day(end, tz)
    snapped_end = end_day if end_day == end else end_day + timedelta(days=1)
    if snapped_end > datetime.now(tz):
        snapped_end = end_day
    return _start_of_day(start, tz), snapped_end


def _start_of_day(moment: datetime, tz: tzinfo) -> datetime:
    """Return midnight in *tz* on the day *moment* falls in."""
    return moment.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0)


def last_instant(end: datetime) -> datetime:
    """Return the last instant a half-open window ending at *end* holds.

    A half-open window stops short of *end*,
    so the day, hour, or minute *end* falls in is not one the window reached:
    a window closing at local midnight is named by the day it reported on,
    not by the day it stops at.
    Callers that name a window by a calendar figure step back
    through here first.

    Everything that takes that step shares this one function,
    so the default email subject and the last row of ``Activity by Day``
    cannot disagree about which day a window ends on.
    """
    return end - _ONE_MICROSECOND


def parse_iso_utc(text: object) -> datetime | None:
    """Parse an ISO 8601 timestamp into an aware datetime, assuming UTC.

    A value carrying no offset is read as UTC
    rather than as the host's local time,
    which is how JupyterHub and Elasticsearch report times.
    Returns *None* for anything that is not a parsable ISO 8601 string,
    so callers can skip unusable records without catching exceptions.
    """
    if not isinstance(text, str) or not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def get_now_ms() -> int:
    """Return the current UTC time as a millisecond epoch timestamp."""
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


@lru_cache(maxsize=128)
def parse_duration(duration_str: str) -> timedelta | None:
    """Parse a duration string into a timedelta.

    Zero and negative durations parse successfully,
    so callers that need a positive one must check the sign themselves.
    Returns *None* if *duration_str* cannot be parsed.

    Results are cached, because the activity metrics parse
    the same few ``meta.interval`` strings once per document.
    """
    seconds = pytimeparse2.parse(duration_str)
    if seconds is None:
        return None
    return seconds if isinstance(seconds, timedelta) else timedelta(seconds=float(seconds))


def parse_duration_required(duration: str) -> timedelta:
    """Parse a required duration string.

    Raises:
        DataShapeError: If *duration* cannot be parsed.
    """
    parsed = parse_duration(duration)
    if parsed is None:
        raise DataShapeError(f"Invalid duration format: {duration!r}")
    return parsed
