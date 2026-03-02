"""Utilities for timezone parsing and reporting time-range computation."""

import re
from datetime import datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_OFFSET_RE = re.compile(r"^([+-])(\d{1,2}):(\d{2})$")


def parse_timezone(tz_str: str) -> tzinfo:
    """Parse a timezone string into a :class:`datetime.tzinfo` object.

    Accepts IANA timezone names (``"America/Chicago"``), common abbreviations
    that are valid IANA keys (``"MST"``), or fixed UTC-offset strings in the
    form ``"+HH:MM"`` / ``"-HH:MM"``.

    Args:
        tz_str: Timezone specification string.

    Returns:
        A ``tzinfo`` instance representing the specified timezone.

    Raises:
        ValueError: If the string cannot be interpreted as a valid timezone.
    """
    # Try IANA name / abbreviation first.
    try:
        return ZoneInfo(tz_str)
    except ZoneInfoNotFoundError, KeyError:
        pass

    # Try fixed UTC offset: ±HH:MM
    if m := _OFFSET_RE.match(tz_str):
        sign = 1 if m.group(1) == "+" else -1
        hours, minutes = int(m.group(2)), int(m.group(3))
        offset = timedelta(hours=hours, minutes=minutes) * sign
        return timezone(offset, name=tz_str)

    raise ValueError(f"Unrecognised timezone: {tz_str!r}")


def compute_time_range(
    duration_td: timedelta,
    time_str: str | None,
    tz: tzinfo,
) -> tuple[datetime, datetime]:
    """Compute the (start, end) datetime pair for a reporting window.

    Args:
        duration_td: Length of the reporting window.
        time_str: Optional ``"HH:MM"`` string.  When given, *end* is the most
            recent occurrence of that wall-clock time in the past 24 hours (in
            *tz*).  When ``None``, *end* is the current moment.
        tz: Timezone in which *time_str* is expressed and to which all returned
            datetimes are localised.

    Returns:
        A ``(start, end)`` pair of timezone-aware datetimes, both in *tz*.
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

    start = end - duration_td
    return start, end


def get_now_ms() -> int:
    """Return the current UTC time as a millisecond epoch timestamp.

    Returns:
        Current time in milliseconds since the Unix epoch
    """
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)
