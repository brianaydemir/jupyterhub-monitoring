"""Builders for report-domain objects used by report commands."""

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, tzinfo
from typing import Any

from app.core.name_utils import ParsedName, parse_name, trailing_domain_key
from app.reports.anonymize import build_pseudonyms
from app.reports.model import Report, ReportSection, TableBlock

_DATETIME_FMT = "%Y-%m-%d %H:%M"


@dataclass(frozen=True)
class SessionActivity:
    """Active server time for a single user session.

    A session is identified by a user together with the server's
    ``server.started`` timestamp; *started* is the raw ISO 8601 string
    reported by JupyterHub, or ``None`` when it was absent.
    """

    user: str
    started: str | None
    seconds: float
    gpu: bool


def _day_seconds() -> dict[str, float]:
    """Return an empty typed day-to-seconds mapping for dataclass defaults."""
    return {}


@dataclass(frozen=True)
class ActivitySummary:  # pylint: disable=too-many-instance-attributes
    """Aggregate usage statistics for a JupyterHub activity report.

    All durations are raw seconds; formatting is applied by the report
    builder.  *per_day_seconds* maps ``"YYYY-MM-DD"`` to the active
    server-time observed on that day, and *mean_concurrency* is averaged
    over snapshots that had at least one active server.
    """

    total_seconds: float = 0.0
    gpu_seconds: float = 0.0
    active_users: int = 0
    total_sessions: int = 0
    gpu_users: int = 0
    gpu_sessions: int = 0
    mean_user_seconds: float = 0.0
    median_user_seconds: float = 0.0
    max_user_seconds: float = 0.0
    mean_sessions_per_user: float = 0.0
    mean_session_seconds: float = 0.0
    peak_concurrency: int = 0
    mean_concurrency: float = 0.0
    per_day_seconds: dict[str, float] = field(default_factory=_day_seconds)


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


def _table_section(section_spec: TabularSectionSpec) -> ReportSection:
    """Build a report section wrapping a single table block."""
    table_block = TableBlock(
        headers=section_spec.headers,
        rows=section_spec.rows,
        right_align_columns=section_spec.right_align_columns,
        attachment_filename=section_spec.attachment_filename,
    )
    return ReportSection(
        heading=section_spec.heading,
        description=section_spec.description,
        blocks=[table_block],
        footnotes=section_spec.footnotes,
    )


def _single_table_report(title: str, section_spec: TabularSectionSpec) -> Report:
    """Build a report with one section and one table block."""
    return Report(title=title, sections=[_table_section(section_spec)])


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


def _identifier_labeler(
    parsed_names: Iterable[ParsedName], anonymize: bool
) -> Callable[[ParsedName], str]:
    """Return a function mapping a parsed name to its display identifier.

    When *anonymize* is set, identifiers are per-institution pseudonyms;
    otherwise the raw uid is used.
    """
    if not anonymize:
        return lambda pn: pn.uid
    pseudonyms = build_pseudonyms((pn.domain, pn.uid) for pn in parsed_names)
    return lambda pn: pseudonyms[(pn.domain, pn.uid)]


def _activity_time(seconds: float) -> str:
    """Format a duration in seconds as HH:MM."""
    total_minutes = int(seconds) // 60
    hours, minutes = divmod(total_minutes, 60)
    return f"{hours}:{minutes:02d}"


def _user_table(  # pylint: disable=too-many-locals
    users: list[dict[str, Any]],
    tz: tzinfo,
    strftime_fmt: str,
    detailed_usernames: bool,
    first_server: dict[str, int],
    anonymize: bool,
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
    ident = _identifier_labeler((pn for _, _, pn in parsed), anonymize)

    if show_method:
        sorted_rows = sorted(
            parsed, key=lambda r: (r[0], r[2].priority, r[2].domain, ident(r[2]))
        )
    else:
        sorted_rows = sorted(
            parsed,
            key=lambda r: (r[0], trailing_domain_key(r[2].domain), r[2].domain, ident(r[2])),
        )

    headers = ["Created", "First server", "Institution", "ID"]
    if show_method:
        headers.append("Login method")

    rows: list[list[str]] = []
    for created, first, pn in sorted_rows:
        row = [created, first, pn.domain, ident(pn)]
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
    anonymize: bool = False,
) -> Report:
    """Build a report object for recently created JupyterHub users.

    *first_server* maps a username to the epoch-seconds time of that
    user's first server start; users absent from the mapping are shown as
    having never started a server.

    When *anonymize* is set, within-institution identifiers are replaced
    with generic pseudonyms; the institution is still shown.
    """
    n = len(users)
    tz = start_time.tzinfo
    if tz is None or start_time.utcoffset() is None:
        raise ValueError("start_time must be timezone-aware")
    if end_time.tzinfo is None or end_time.utcoffset() is None:
        raise ValueError("end_time must be timezone-aware")

    headers, rows = _user_table(
        users, tz, strftime_fmt, detailed_usernames, first_server or {}, anonymize
    )
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


def _activity_name_key(pn: ParsedName, show_method: bool, ident: str) -> tuple[Any, ...]:
    """Return a sort key for a user in the activity table.

    *ident* is the value shown in the ID column (a pseudonym when
    anonymizing) so ordering does not leak the raw identifier.
    """
    if show_method:
        return (pn.priority, pn.domain, ident)
    return (trailing_domain_key(pn.domain), pn.domain, ident)


def _session_sort_key(started: str | None) -> tuple[bool, str]:
    """Return a sort key ordering sessions by start time, blanks last."""
    return (started is None or not started, started or "")


def _activity_table(
    sessions: list[SessionActivity],
    tz: tzinfo,
    strftime_fmt: str,
    detailed_usernames: bool,
    anonymize: bool,
) -> tuple[list[str], list[list[str]]]:
    """Build headers and rows for the per-session activity table."""
    parsed_names = {session.user: parse_name(session.user) for session in sessions}
    show_method = _show_method(
        [(pn.domain, pn.uid) for pn in parsed_names.values()], detailed_usernames
    )
    ident = _identifier_labeler(parsed_names.values(), anonymize)

    sorted_sessions = sorted(
        sessions,
        key=lambda s: (
            _activity_name_key(parsed_names[s.user], show_method, ident(parsed_names[s.user])),
            _session_sort_key(s.started),
        ),
    )

    headers = ["Started", "Time (HH:MM)", "Institution", "ID"]
    if show_method:
        headers.append("Login method")
    headers.append("GPU")

    rows: list[list[str]] = []
    for session in sorted_sessions:
        pn = parsed_names[session.user]
        started = (
            _format_created(session.started, strftime_fmt, tz) if session.started else "n/a"
        )
        row = [started, _activity_time(session.seconds), pn.domain, ident(pn)]
        if show_method:
            row.append(pn.login_method)
        row.append("Yes" if session.gpu else "No")
        rows.append(row)
    return headers, rows


def _summary_rows(summary: ActivitySummary) -> list[list[str]]:
    """Build the ``Metric``/``Value`` rows for the summary table."""
    total = summary.total_seconds
    gpu = summary.gpu_seconds
    pct = round(100 * gpu / total) if total > 0 else 0
    if summary.per_day_seconds:
        day, secs = max(summary.per_day_seconds.items(), key=lambda kv: kv[1])
        busiest = f"{day} ({_activity_time(secs)})"
    else:
        busiest = "n/a"
    return [
        ["Total active time", _activity_time(total)],
        ["Active users", str(summary.active_users)],
        ["Sessions", str(summary.total_sessions)],
        ["GPU active time", f"{_activity_time(gpu)} ({pct}%)"],
        ["CPU active time", _activity_time(total - gpu)],
        ["GPU users", str(summary.gpu_users)],
        ["GPU sessions", str(summary.gpu_sessions)],
        ["Mean time per user", _activity_time(summary.mean_user_seconds)],
        ["Median time per user", _activity_time(summary.median_user_seconds)],
        ["Max time (one user)", _activity_time(summary.max_user_seconds)],
        ["Mean sessions per user", f"{summary.mean_sessions_per_user:.1f}"],
        ["Mean session length", _activity_time(summary.mean_session_seconds)],
        ["Peak concurrent servers", str(summary.peak_concurrency)],
        ["Mean concurrent servers", f"{summary.mean_concurrency:.1f}"],
        ["Busiest day", busiest],
    ]


def _daily_rows(summary: ActivitySummary) -> list[list[str]]:
    """Build the ``Day``/``Active time`` rows for the per-day table."""
    return [
        [day, _activity_time(summary.per_day_seconds[day])]
        for day in sorted(summary.per_day_seconds)
    ]


def _stats_section(
    heading: str,
    description: str,
    headers: list[str],
    rows: list[list[str]],
    footnotes: list[str] | None = None,
) -> ReportSection:
    """Build a report section with one right-aligned-value stats table."""
    return ReportSection(
        heading=heading,
        description=description,
        blocks=[TableBlock(headers=headers, rows=rows, right_align_columns={1})],
        footnotes=footnotes or [],
    )


def _summary_sections(
    summary: ActivitySummary,
    start_time: datetime,
    end_time: datetime,
) -> list[ReportSection]:
    """Build the ``Summary`` and (optional) ``Activity by Day`` sections."""
    window = (
        f"Usage from {start_time.strftime(_DATETIME_FMT)} "
        f"to {end_time.strftime(_DATETIME_FMT)}."
    )
    concurrency_note = (
        "Mean concurrent servers is averaged over snapshots that had "
        "at least one active server."
    )
    sections = [
        _stats_section(
            "Summary",
            window,
            ["Metric", "Value"],
            _summary_rows(summary),
            footnotes=[concurrency_note],
        )
    ]
    if summary.per_day_seconds:
        sections.append(
            _stats_section(
                "Activity by Day",
                "Active server time per day.",
                ["Day", "Active time"],
                _daily_rows(summary),
            )
        )
    return sections


def build_activity_report(
    sessions: list[SessionActivity],
    summary: ActivitySummary,
    start_time: datetime,
    end_time: datetime,
    tz_name: str,
    strftime_fmt: str,
    detailed_usernames: bool = False,
    anonymize: bool = False,
) -> Report:
    """Build a report object for per-session JupyterHub server activity.

    The report opens with a ``Summary`` section of aggregate usage
    statistics and, when daily data is available, an ``Activity by Day``
    section, followed by the ``User Activity`` table.  Each activity row is
    one user session, identified by the user and the server's start time;
    the ``GPU`` column reflects whether the session requested a GPU.

    When *anonymize* is set, within-institution identifiers are replaced
    with generic pseudonyms; the institution is still shown.

    Raises:
        ValueError: If *start_time* or *end_time* is not
            timezone-aware.
    """
    if start_time.tzinfo is None or start_time.utcoffset() is None:
        raise ValueError("start_time must be timezone-aware")
    if end_time.tzinfo is None or end_time.utcoffset() is None:
        raise ValueError("end_time must be timezone-aware")

    tz = start_time.tzinfo
    headers, rows = _activity_table(sessions, tz, strftime_fmt, detailed_usernames, anonymize)
    description = _summary_description(
        count=len(sessions),
        singular_phrase="session was active",
        plural_phrase="sessions were active",
        zero_phrase="No sessions were active",
        start_time=start_time,
        end_time=end_time,
    )

    sections = _summary_sections(summary, start_time, end_time)
    sections.append(
        _table_section(
            TabularSectionSpec(
                heading="User Activity",
                description=description,
                headers=headers,
                rows=rows,
                right_align_columns={1},
                attachment_filename="activity.csv",
                footnotes=[f"Timezone: {tz_name}"],
            )
        )
    )

    return Report(title="JupyterHub Activity Report", sections=sections)
