"""Builders for report-domain objects used by report commands."""

from dataclasses import dataclass, field
from datetime import datetime, tzinfo
from typing import Any

from app.core.name_utils import ParsedName, parse_name, trailing_domain_key
from app.reports.model import Report, ReportSection, TableBlock

_DATETIME_FMT = "%Y-%m-%d %H:%M"


def _string_list() -> list[str]:
    """Return an empty typed string list for dataclass defaults."""
    return []


def _int_set() -> set[int]:
    """Return an empty typed integer set for dataclass defaults."""
    return set()


@dataclass(frozen=True)
class TabularSectionSpec:
    """Description of a report section containing a single table block."""

    heading: str
    description: str
    headers: list[str]
    rows: list[list[str]]
    attachment_filename: str
    right_align_columns: set[int] = field(default_factory=_int_set)
    footnotes: list[str] = field(default_factory=_string_list)


def _single_table_report(title: str, section_spec: TabularSectionSpec) -> Report:
    """Build a report with one section and one table block."""
    table_block = TableBlock(
        headers=section_spec.headers,
        rows=section_spec.rows,
        right_align_columns=section_spec.right_align_columns,
        attachment_filename=section_spec.attachment_filename,
    )
    return Report(
        title=title,
        sections=[
            ReportSection(
                heading=section_spec.heading,
                description=section_spec.description,
                blocks=[table_block],
                footnotes=section_spec.footnotes,
            )
        ],
    )


def _summary_description(
    *,
    count: int,
    singular_phrase: str,
    plural_phrase: str,
    zero_phrase: str,
    start_time: datetime,
    end_time: datetime,
) -> str:
    """Build a count-aware summary sentence for a report section."""
    if count == 0:
        return (
            f"{zero_phrase} between {start_time.strftime(_DATETIME_FMT)} "
            f"and {end_time.strftime(_DATETIME_FMT)}."
        )
    phrase = singular_phrase if count == 1 else plural_phrase
    return (
        f"{count} {phrase} from {start_time.strftime(_DATETIME_FMT)} "
        f"to {end_time.strftime(_DATETIME_FMT)}."
    )


def _format_created(created_str: str, strftime_fmt: str, tz: tzinfo) -> str:
    """Format an ISO 8601 creation timestamp for display."""
    try:
        dt = datetime.fromisoformat(created_str).astimezone(tz)
        return dt.strftime(strftime_fmt)
    except ValueError, TypeError, AttributeError:
        return created_str


def _format_first_server(ts: int | None, strftime_fmt: str, tz: tzinfo) -> str:
    """Format a user's first server-start time, or ``"n/a"`` if never."""
    if ts is None:
        return "n/a"
    return datetime.fromtimestamp(ts, tz).strftime(strftime_fmt)


def _show_method(domain_id_pairs: list[tuple[str, str]], detailed_usernames: bool) -> bool:
    """Return whether the Login method column should be shown.

    Shown when explicitly requested or when (domain, uid) pairs
    alone would be ambiguous.
    """
    return detailed_usernames or len(domain_id_pairs) != len(set(domain_id_pairs))


def _activity_time(seconds: float) -> str:
    """Format a duration in seconds as HH:MM."""
    total_minutes = int(seconds) // 60
    hours, minutes = divmod(total_minutes, 60)
    return f"{hours}:{minutes:02d}"


def _user_table(
    users: list[dict[str, Any]],
    tz: tzinfo,
    strftime_fmt: str,
    detailed_usernames: bool,
    first_server: dict[str, int],
) -> tuple[list[str], list[list[str]]]:
    """Build headers and rows for the new-users table."""
    parsed = [
        (
            _format_created(user.get("created", ""), strftime_fmt, tz),
            _format_first_server(first_server.get(user.get("name", "")), strftime_fmt, tz),
            parse_name(user.get("name", "")),
        )
        for user in users
    ]
    show_method = _show_method([(pn.domain, pn.uid) for _, _, pn in parsed], detailed_usernames)
    if show_method:
        sorted_rows = sorted(parsed, key=lambda r: (r[0], r[2].priority, r[2].domain, r[2].uid))
    else:
        sorted_rows = sorted(
            parsed,
            key=lambda r: (r[0], trailing_domain_key(r[2].domain), r[2].domain, r[2].uid),
        )

    headers = ["Created", "First server", "Institution", "ID"]
    if show_method:
        headers.append("Login method")

    rows: list[list[str]] = []
    for created, first, pn in sorted_rows:
        row = [created, first, pn.domain, pn.uid]
        if show_method:
            row.append(pn.login_method)
        rows.append(row)
    return headers, rows


def build_new_users_report(
    users: list[dict[str, Any]],
    start_time: datetime,
    end_time: datetime,
    tz_name: str,
    strftime_fmt: str,
    detailed_usernames: bool = False,
    first_server: dict[str, int] | None = None,
) -> Report:
    """Build a report object for recently created JupyterHub users.

    *first_server* maps a username to the epoch-seconds time of that
    user's first server start; users absent from the mapping are shown as
    having never started a server.
    """
    n = len(users)
    tz = start_time.tzinfo
    if tz is None or start_time.utcoffset() is None:
        raise ValueError("start_time must be timezone-aware")
    if end_time.tzinfo is None or end_time.utcoffset() is None:
        raise ValueError("end_time must be timezone-aware")

    headers, rows = _user_table(users, tz, strftime_fmt, detailed_usernames, first_server or {})
    description = _summary_description(
        count=n,
        singular_phrase="new user created",
        plural_phrase="new users created",
        zero_phrase="No new users created",
        start_time=start_time,
        end_time=end_time,
    )
    return _single_table_report(
        title="JupyterHub New Users Report",
        section_spec=TabularSectionSpec(
            heading="New Users",
            description=description,
            headers=headers,
            rows=rows,
            attachment_filename="new-users.csv",
            footnotes=[f"Timezone: {tz_name}"],
        ),
    )


def _activity_name_key(pn: ParsedName, show_method: bool) -> tuple[Any, ...]:
    """Return a sort key for a user in the activity table."""
    if show_method:
        return (pn.priority, pn.domain, pn.uid)
    return (trailing_domain_key(pn.domain), pn.domain, pn.uid)


def _activity_table(
    totals: dict[str, float], detailed_usernames: bool
) -> tuple[list[str], list[list[str]]]:
    """Build headers and rows for the activity table."""
    parsed_names = {user: parse_name(user) for user in totals}
    show_method = _show_method(
        [(pn.domain, pn.uid) for pn in parsed_names.values()], detailed_usernames
    )
    sorted_items = sorted(
        totals.items(),
        key=lambda item: (-item[1], _activity_name_key(parsed_names[item[0]], show_method)),
    )

    headers = ["Time (HH:MM)", "Institution", "ID"]
    if show_method:
        headers.append("Login method")

    rows: list[list[str]] = []
    for user, seconds in sorted_items:
        pn = parsed_names[user]
        row = [_activity_time(seconds), pn.domain, pn.uid]
        if show_method:
            row.append(pn.login_method)
        rows.append(row)
    return headers, rows


def build_activity_report(
    totals: dict[str, float],
    start_time: datetime,
    end_time: datetime,
    tz_name: str,
    detailed_usernames: bool = False,
) -> Report:
    """Build a report object for per-user JupyterHub server activity.

    Raises:
        ValueError: If *start_time* or *end_time* is not
            timezone-aware.
    """
    if start_time.tzinfo is None or start_time.utcoffset() is None:
        raise ValueError("start_time must be timezone-aware")
    if end_time.tzinfo is None or end_time.utcoffset() is None:
        raise ValueError("end_time must be timezone-aware")

    n = len(totals)
    headers, rows = _activity_table(totals, detailed_usernames)
    description = _summary_description(
        count=n,
        singular_phrase="user was active",
        plural_phrase="users were active",
        zero_phrase="No users were active",
        start_time=start_time,
        end_time=end_time,
    )

    return _single_table_report(
        title="JupyterHub Activity Report",
        section_spec=TabularSectionSpec(
            heading="User Activity",
            description=description,
            headers=headers,
            rows=rows,
            right_align_columns={0},
            attachment_filename="activity.csv",
            footnotes=[f"Timezone: {tz_name}"],
        ),
    )
