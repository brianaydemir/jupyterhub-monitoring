"""Builders for report-domain objects used by report commands."""

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, tzinfo
from typing import Any

from app.core.dataclass_defaults import empty_int_set, empty_string_list
from app.core.name_utils import ParsedName, parse_name, trailing_domain_key
from app.core.time_utils import DATETIME_FMT, format_timezone, parse_iso_utc
from app.reports.anonymize import build_pseudonyms
from app.reports.model import Cell, Duration, Fixed, Percent, Report, ReportSection, TableBlock
from app.usage.activity import (
    ActivitySummary,
    SessionActivity,
    UserActivity,
    aggregate_user_activity,
)
from app.usage.new_users import AdoptionSummary, summarize_adoption


@dataclass(frozen=True)
class TabularSectionSpec:
    """Description of a report section containing a single table block."""

    heading: str
    description: str
    headers: list[str]
    rows: list[list[Cell]]
    attachment_filename: str
    right_align_columns: set[int] = field(default_factory=empty_int_set)
    footnotes: list[str] = field(default_factory=empty_string_list)


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


def _stats_section(
    heading: str,
    description: str,
    headers: list[str],
    rows: list[list[Cell]],
    footnotes: list[str] | None = None,
) -> ReportSection:
    """Build a report section with one right-aligned-value stats table."""
    return ReportSection(
        heading=heading,
        description=description,
        blocks=[TableBlock(headers=headers, rows=rows, right_align_columns={1})],
        footnotes=footnotes or [],
    )


def _report_zone(start_time: datetime, end_time: datetime) -> tzinfo:
    """Return the zone a reporting window is expressed in.

    The window is the report's single source for the zone: every
    timestamp is rendered in it, and :func:`_timezone_footnote` names it.
    A naive bound would leave both undefined, so it is rejected here
    rather than silently rendered in whatever the host's clock says.

    Raises:
        ValueError: If either bound is not timezone-aware.
    """
    tz = start_time.tzinfo
    if tz is None or start_time.utcoffset() is None:
        raise ValueError("start_time must be timezone-aware")
    if end_time.tzinfo is None or end_time.utcoffset() is None:
        raise ValueError("end_time must be timezone-aware")
    return tz


def _timezone_footnote(tz: tzinfo) -> str:
    """Return the footnote naming the zone the timestamps are rendered in."""
    return f"Timezone: {format_timezone(tz)}"


# The unit every duration in a section is shown in, stated once per
# table rather than in each column header.
_DURATION_FOOTNOTE = "Times are HH:MM."


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
            f"{zero_phrase} between {start_time.strftime(DATETIME_FMT)} "
            f"and {end_time.strftime(DATETIME_FMT)}."
        )
    phrase = singular_phrase if count == 1 else plural_phrase
    return (
        f"{count} {phrase} from {start_time.strftime(DATETIME_FMT)} "
        f"to {end_time.strftime(DATETIME_FMT)}."
    )


def _format_iso_timestamp(value: str, strftime_fmt: str, tz: tzinfo) -> str:
    """Format an ISO 8601 timestamp for display in the report's zone.

    An unparsable value is passed through unchanged, so an unexpected
    format shows up in the report rather than being silently dropped.
    :func:`~app.core.time_utils.parse_iso_utc` reads a value carrying no
    offset as UTC, matching how
    :func:`~app.usage.new_users.filter_new_users` selects rows.
    """
    parsed = parse_iso_utc(value)
    if parsed is None:
        return str(value)
    return parsed.astimezone(tz).strftime(strftime_fmt)


# The report's vocabulary for a value it knows to be absent, kept
# together so a change to one is a change to all three. "n/a" is a
# value the source did not carry. The other two are absences that are
# themselves data — a server with no name is the default one, and a
# username in none of the known forms carries no identity — and are
# named rather than blanked, since a blank cell reads as data the
# report failed to fill in. No real domain or login method takes the
# form of the last.
_NOT_APPLICABLE = "n/a"
_DEFAULT_SERVER = "(default)"
_NO_IDENTITY = "(none)"


def _format_first_server(ts: int | None, strftime_fmt: str, tz: tzinfo) -> str:
    """Format a user's first server-start time, or ``"n/a"`` if never."""
    if ts is None:
        return _NOT_APPLICABLE
    return datetime.fromtimestamp(ts, tz).strftime(strftime_fmt)


def _show_method(domain_id_pairs: list[tuple[str, str]], detailed_usernames: bool) -> bool:
    """Return whether the Login method column should be shown.

    Shown when explicitly requested or when the pairs would be ambiguous.
    Callers pass the *displayed* (Institution, ID) pairs, so that two
    users whose pseudonyms collide under ``--anonymize`` still get the
    disambiguating column.
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


def _identity_cell(value: str) -> str:
    """Return an identity cell, naming an absent value rather than blanking it.

    :func:`~app.core.name_utils.parse_name` leaves both the domain and
    the login method empty for a name in none of the forms it knows, and
    a blank cell would read as data the report failed to fill in. The
    marker belongs here rather than in that function, whose empty string
    is the honest domain and is what
    :func:`~app.reports.anonymize.build_pseudonyms` keys a pseudonym on.
    """
    return value or _NO_IDENTITY


@dataclass(frozen=True)
class _UserDisplay:
    """The identity columns a user table presents its usernames in.

    The new-users table and the two activity tables each read one
    instance, so no two of them can disagree about a user's displayed
    identifier, about whether the ``Login method`` column is shown, or
    about how an absent domain or login method renders.
    """

    parsed: dict[str, ParsedName]
    label: Callable[[ParsedName], str]
    show_method: bool

    def headers(self) -> list[str]:
        """Return the identity column headers."""
        headers = ["Institution", "ID"]
        if self.show_method:
            headers.append("Login method")
        return headers

    def cells(self, user: str) -> list[str]:
        """Return the identity cells for a username."""
        pn = self.parsed[user]
        cells = [_identity_cell(pn.domain), self.label(pn)]
        if self.show_method:
            cells.append(_identity_cell(pn.login_method))
        return cells

    def name_key(self, user: str) -> tuple[Any, ...]:
        """Return the identity key a table breaks a tie on.

        The key reads the value shown in the ID column (a pseudonym when
        anonymizing), so ordering does not leak the raw identifier.

        With the ``Login method`` column shown, rows group by login
        method first; otherwise by institution, with the trailing domains
        last. Either way an unrecognized name sorts last.
        """
        pn = self.parsed[user]
        if self.show_method:
            return (pn.priority, pn.domain, self.label(pn))
        return (trailing_domain_key(pn.domain), pn.domain, self.label(pn))


def _user_display(
    names: Iterable[str], detailed_usernames: bool, anonymize: bool
) -> _UserDisplay:
    """Build the display identity for the usernames a table lists.

    The parse holds one entry per distinct name, so that a user repeated
    in the input cannot masquerade as two users sharing an
    (Institution, ID) and turn the ``Login method`` column on by itself.
    """
    parsed = {name: parse_name(name) for name in dict.fromkeys(names)}
    label = _identifier_labeler(parsed.values(), anonymize)
    show_method = _show_method(
        [(pn.domain, label(pn)) for pn in parsed.values()], detailed_usernames
    )
    return _UserDisplay(parsed=parsed, label=label, show_method=show_method)


def _window_length(summary: ActivitySummary) -> Duration:
    """Return the reporting window's length, in the units of the durations.

    Both the ``Reporting window`` row and the section's own sentence
    read this, so the two cannot disagree. A window of no length, or an
    inverted one, is floored to no time at all rather than left as the
    negative duration it would otherwise display as:
    :func:`~app.usage.activity.summarize_activity` already floors such a
    window, but a summary built by hand need not have.
    """
    return Duration(max(summary.window_seconds, 0.0))


def _user_table(  # pylint: disable=too-many-locals
    users: list[dict[str, Any]],
    tz: tzinfo,
    strftime_fmt: str,
    detailed_usernames: bool,
    first_server: dict[str, int] | None,
    anonymize: bool,
) -> tuple[list[str], list[list[Cell]]]:
    """Build headers and rows for the new-users table.

    A *first_server* of ``None`` drops the "First server" column, rather
    than filling it with ``"n/a"`` for every row.

    Rows are ordered by creation date, and an equal date breaks on the
    same identity key the activity tables break an equal total on. A row
    is one entry of *users*, not one username, so a user repeated in the
    input is listed as many times as it appears.
    """
    show_first_server = first_server is not None
    lookup = first_server or {}
    dated = [
        (
            _format_iso_timestamp(user.get("created", ""), strftime_fmt, tz),
            _format_first_server(lookup.get(user.get("name", "")), strftime_fmt, tz),
            user.get("name", ""),
        )
        for user in users
    ]
    display = _user_display((name for _, _, name in dated), detailed_usernames, anonymize)
    ordered = sorted(dated, key=lambda r: (r[0], display.name_key(r[2])))

    headers = ["Created"]
    if show_first_server:
        headers.append("First server")
    headers.extend(display.headers())

    rows: list[list[Cell]] = []
    for created, first, name in ordered:
        row: list[Cell] = [created]
        if show_first_server:
            row.append(first)
        row.extend(display.cells(name))
        rows.append(row)
    return headers, rows


def _adoption_rows(adoption: AdoptionSummary) -> list[list[Cell]]:
    """Build the ``Metric``/``Value`` rows for the adoption table.

    The population leads, then the split of it, then how quickly the
    users who did start got started. The median row is dropped rather
    than filled with ``"n/a"`` when no elapsed time could be measured,
    since a median over nothing is not a figure a reader can act on.
    """
    rows: list[list[Cell]] = [
        ["New users", adoption.new_users],
        ["Started a server", adoption.started],
        ["Never started a server", adoption.never_started],
        ["Started same day", adoption.same_day],
    ]
    if adoption.median_seconds_to_first is not None:
        rows.append(["Median time to first server", Duration(adoption.median_seconds_to_first)])
    return rows


def _adoption_section(adoption: AdoptionSummary) -> ReportSection:
    """Build the ``Adoption`` section of the new-users report.

    The unit note goes on only when a duration is actually shown, since
    ``Median time to first server`` is the section's one duration and is
    dropped when no elapsed time could be measured; a note explaining a
    format no visible cell is read in would be noise. It is decided from
    the rows rather than from *adoption*, so the note and the table
    cannot disagree about whether that row is there.
    """
    rows = _adoption_rows(adoption)
    shows_duration = any(isinstance(cell, Duration) for row in rows for cell in row)
    return _stats_section(
        "Adoption",
        "How many of the new users went on to start a server.",
        ["Metric", "Value"],
        rows,
        footnotes=[_DURATION_FOOTNOTE] if shows_duration else [],
    )


def build_new_users_report(
    users: list[dict[str, Any]],
    start_time: datetime,
    end_time: datetime,
    strftime_fmt: str,
    detailed_usernames: bool = False,
    first_server: dict[str, int] | None = None,
    anonymize: bool = False,
) -> Report:
    """Build a report object for recently created JupyterHub users.

    An ``Adoption`` section leads the report whenever *first_server* was
    supplied, counting how many of the new users went on to start a
    server and how quickly; the ``New Users`` table follows it.

    The reporting window supplies the zone: timestamps are rendered in
    *start_time*'s zone, which is also the one the report's footnote
    names. :func:`~app.usage.new_users.summarize_adoption` reads that
    same zone, so its same-day count agrees with the dates the table
    shows.

    *first_server* maps a username to the epoch-seconds time of that
    user's first server start; users absent from the mapping are shown as
    having never started a server. Pass ``None`` when Elasticsearch was
    not consulted at all, which omits the "First server" column and the
    ``Adoption`` section: with nothing to date, every figure in that
    section would understate users who may well have started servers.

    When *anonymize* is set, within-institution identifiers are replaced
    with generic pseudonyms; the institution is still shown.

    Raises:
        ValueError: If *start_time* or *end_time* is not
            timezone-aware.
    """
    tz = _report_zone(start_time, end_time)
    headers, rows = _user_table(
        users, tz, strftime_fmt, detailed_usernames, first_server, anonymize
    )
    description = _summary_description(
        count=len(users),
        singular_phrase="new user created",
        plural_phrase="new users created",
        zero_phrase="No new users created",
        start_time=start_time,
        end_time=end_time,
    )

    sections: list[ReportSection] = []
    if first_server is not None:
        sections.append(_adoption_section(summarize_adoption(users, first_server, tz)))
    sections.append(
        _table_section(
            TabularSectionSpec(
                heading="New Users",
                description=description,
                headers=headers,
                rows=rows,
                attachment_filename="new-users.csv",
                footnotes=[_timezone_footnote(tz)],
            )
        )
    )

    return Report(title="JupyterHub New Users Report", sections=sections)


def _activity_order(users: list[UserActivity], display: _UserDisplay) -> dict[str, int]:
    """Return the position each username takes in both activity tables.

    Users are ordered by total active time, most active first, so that
    the report leads with whoever consumed the most rather than with
    whoever sorts first alphabetically. An equal total breaks on
    :meth:`_UserDisplay.name_key`, which reads the displayed identifier,
    so neither the order of two equal totals varies between runs nor
    does any ordering follow a value ``--anonymize`` hides.

    One order serves both tables, which is what lets ``User Activity``
    place a user's sessions where ``User Totals`` places the user.
    """
    ordered = sorted(users, key=lambda u: (-u.seconds, display.name_key(u.user)))
    return {user.user: position for position, user in enumerate(ordered)}


def _session_sort_key(session: SessionActivity) -> tuple[bool, str, str]:
    """Return a sort key ordering sessions by start time, blanks last.

    The server name breaks ties, so a user's concurrent named servers
    keep a stable order.
    """
    started = session.started or ""
    return (not started, started, session.server_name or "")


# The header of the per-session table's one right-aligned column. The
# optional ``Server`` column moves it, so it is found by name.
_ACTIVITY_TIME_HEADER = "Time"


def _show_server(sessions: list[SessionActivity]) -> bool:
    """Return whether the ``Server`` column should be shown.

    Shown only when a session names a server. Most deployments run
    nothing but default servers, whose column would read the same on
    every row and so distinguish nothing.
    """
    return any(session.server_name for session in sessions)


def _server_label(server_name: str | None) -> str:
    """Return the ``Server`` cell for a session's server name.

    The default server carries an empty name, which reads as
    ``"(default)"`` rather than as a blank cell a reader would take for
    missing data. ``None`` is instead a document that did not say, and
    reads as ``"n/a"``, as an absent value does elsewhere in the report.
    """
    if server_name is None:
        return _NOT_APPLICABLE
    return server_name or _DEFAULT_SERVER


def _activity_table(
    sessions: list[SessionActivity],
    display: _UserDisplay,
    order: dict[str, int],
    tz: tzinfo,
    strftime_fmt: str,
) -> tuple[list[str], list[list[Cell]]]:
    """Build headers and rows for the per-session activity table.

    Users appear in *order*, the one ``User Totals`` reads, so a heavy
    user's sessions sit where that table puts the user;
    :func:`_session_sort_key` orders the rows within each user.

    ``Server`` follows ``Started``, so that the two columns naming the
    session sit together ahead of the ones naming the user, and is
    present only when :func:`_show_server` finds a named server: without
    it, two of one user's concurrent servers render as identical rows.
    """
    sorted_sessions = sorted(
        sessions,
        key=lambda s: (order[s.user], _session_sort_key(s)),
    )
    show_server = _show_server(sessions)

    headers = ["Started"]
    if show_server:
        headers.append("Server")
    headers.extend([_ACTIVITY_TIME_HEADER, *display.headers(), "GPU"])

    rows: list[list[Cell]] = []
    for session in sorted_sessions:
        started = (
            _format_iso_timestamp(session.started, strftime_fmt, tz)
            if session.started
            else _NOT_APPLICABLE
        )
        row: list[Cell] = [started]
        if show_server:
            row.append(_server_label(session.server_name))
        row.extend(
            [
                Duration(session.seconds),
                *display.cells(session.user),
                "Yes" if session.gpu else "No",
            ]
        )
        rows.append(row)
    return headers, rows


def _user_totals_section(
    users: list[UserActivity], display: _UserDisplay, order: dict[str, int]
) -> ReportSection:
    """Build the ``User Totals`` section, one row per active user.

    The order is *order*, by total active time and most active first,
    and the per-session table reads that same mapping, so the two tables
    list their users as one sequence.

    A report with no sessions still gets the section, headers and no
    rows, as ``User Activity`` does: both are that report's per-user
    view, and one of them vanishing would say that the report had
    nothing to say about users rather than that no user was active.
    """
    ordered = sorted(users, key=lambda u: order[u.user])
    headers = [*display.headers(), "Sessions", "Time", "GPU time"]
    rows: list[list[Cell]] = [
        [
            *display.cells(user.user),
            user.sessions,
            Duration(user.seconds),
            Duration(user.gpu_seconds),
        ]
        for user in ordered
    ]
    return _table_section(
        TabularSectionSpec(
            heading="User Totals",
            description="Total active time per user, most active first.",
            headers=headers,
            rows=rows,
            right_align_columns={len(headers) - 3, len(headers) - 2, len(headers) - 1},
            attachment_filename="user-totals.csv",
            footnotes=[_DURATION_FOOTNOTE],
        )
    )


def _summary_rows(summary: ActivitySummary) -> list[list[Cell]]:
    """Build the ``Metric``/``Value`` rows for the summary table.

    The window's length leads, so that a reader meets the scale the
    quantities below it are measured against before the quantities.

    Every row states one quantity, so that a spreadsheet can sort or
    chart any of them: the GPU time and its share of the total are two
    rows, as the busiest day and that day's time are.

    The busiest day is the busiest *active* one. When no day saw
    activity, both of its rows read ``"n/a"``.
    """
    total = summary.total_seconds
    gpu = summary.gpu_seconds
    share = Percent(gpu / total if total > 0 else 0.0)
    day, secs = max(summary.per_day_seconds.items(), key=lambda kv: kv[1], default=("", 0.0))
    busiest: Cell = day if secs > 0 else _NOT_APPLICABLE
    busiest_time: Cell = Duration(secs) if secs > 0 else _NOT_APPLICABLE
    return [
        ["Reporting window", _window_length(summary)],
        ["Total active time", Duration(total)],
        ["Active users", summary.active_users],
        ["Sessions", summary.total_sessions],
        ["GPU active time", Duration(gpu)],
        ["GPU share of total", share],
        ["CPU active time", Duration(total - gpu)],
        ["GPU users", summary.gpu_users],
        ["GPU sessions", summary.gpu_sessions],
        ["Mean time per user", Duration(summary.mean_user_seconds)],
        ["Median time per user", Duration(summary.median_user_seconds)],
        ["Max time (one user)", Duration(summary.max_user_seconds)],
        ["Mean sessions per user", Fixed(summary.mean_sessions_per_user, digits=1)],
        ["Mean session length", Duration(summary.mean_session_seconds)],
        ["Peak concurrent servers", summary.peak_concurrency],
        ["Mean concurrent servers (active)", Fixed(summary.mean_concurrency_active, digits=2)],
        ["Mean concurrent servers (window)", Fixed(summary.mean_concurrency_window, digits=2)],
        ["Busiest day", busiest],
        ["Busiest day active time", busiest_time],
    ]


def _daily_rows(summary: ActivitySummary) -> list[list[Cell]]:
    """Build the ``Day``/``Active time`` rows for the per-day table.

    :func:`~app.usage.activity.summarize_activity` supplies a day for
    every day of the window, so sorting the keys yields a contiguous
    series in which a quiet day reads ``"0:00"`` rather than being
    skipped over.
    """
    return [
        [day, Duration(summary.per_day_seconds[day])] for day in sorted(summary.per_day_seconds)
    ]


def _hubs_footnote(hubs: set[str]) -> str:
    """Return the footnote for a summary that spans more than one hub.

    The per-hub-push claim is scoped to the two figures it holds for:
    the window mean divides a total that already spans every hub.
    """
    return (
        f"Figures aggregate across hubs: {', '.join(sorted(hubs))}. "
        "Peak and mean (active) concurrency are measured per hub push."
    )


def _summary_sections(
    summary: ActivitySummary,
    start_time: datetime,
    end_time: datetime,
) -> list[ReportSection]:
    """Build the ``Summary`` and (optional) ``Activity by Day`` sections.

    The description states the window's length as well as its bounds,
    so that the prose and the ``Reporting window`` row agree. A report
    drawn from several hubs says so, since its figures combine them; one
    hub, or none named, adds no such note.

    The daily section is present whenever the summary holds a day, which
    a summary computed over a window of any length does. What is dropped
    is therefore a window holding no day at all rather than a window that
    saw no activity: the latter is a real, if quiet, series, and a table
    of zeros states that more plainly than an absent section does.
    """
    window = (
        f"Usage from {start_time.strftime(DATETIME_FMT)} "
        f"to {end_time.strftime(DATETIME_FMT)} "
        f"({_window_length(summary).display()})."
    )
    concurrency_note = (
        '"(active)" averages over snapshots that had at least one active '
        'server; "(window)" averages over the whole reporting window.'
    )
    footnotes = [_DURATION_FOOTNOTE, concurrency_note]
    if len(summary.hubs) > 1:
        footnotes.append(_hubs_footnote(summary.hubs))
    sections = [
        _stats_section(
            "Summary",
            window,
            ["Metric", "Value"],
            _summary_rows(summary),
            footnotes=footnotes,
        )
    ]
    if summary.per_day_seconds:
        sections.append(
            _stats_section(
                "Activity by Day",
                "Active server time per day.",
                ["Day", "Active time"],
                _daily_rows(summary),
                footnotes=[_DURATION_FOOTNOTE],
            )
        )
    return sections


def build_activity_report(
    sessions: list[SessionActivity],
    summary: ActivitySummary,
    start_time: datetime,
    end_time: datetime,
    strftime_fmt: str,
    detailed_usernames: bool = False,
    anonymize: bool = False,
) -> Report:
    """Build a report object for per-session JupyterHub server activity.

    The report opens with a ``Summary`` section of aggregate usage
    statistics and, when the summary covers at least one day, an
    ``Activity by Day`` section listing every one of them, followed by
    the ``User Totals`` and ``User Activity`` tables. ``User Totals``
    holds one row per user, most active first; each ``User Activity``
    row is one user session, identified by the user, the named server,
    and that server's start time; the ``GPU`` column reflects whether
    the session requested a GPU. ``User Activity`` names the server in
    a ``Server`` column whenever a session ran on a named one, so that
    two of a user's concurrent servers cannot read as one row printed
    twice; ``User Totals`` gains no such column, since it sums a user's
    servers together. Both tables take their user order from one
    :func:`_activity_order`, so a user found in ``User Totals`` is found
    at the same place in ``User Activity``, that user's sessions
    ordered by start time within it.

    The reporting window supplies the zone: timestamps are rendered in
    *start_time*'s zone, which is also the one the report's footnote
    names.

    *summary* must have been computed over that same window: the
    ``Reporting window`` row and the ``Summary`` description read the
    window's length from ``summary.window_seconds``, while its bounds
    come from *start_time* and *end_time*, so a summary computed over
    some other window makes the two disagree.

    When *anonymize* is set, within-institution identifiers are replaced
    with generic pseudonyms; the institution is still shown.

    Raises:
        ValueError: If *start_time* or *end_time* is not
            timezone-aware.
    """
    tz = _report_zone(start_time, end_time)
    users = aggregate_user_activity(sessions)
    display = _user_display((user.user for user in users), detailed_usernames, anonymize)
    order = _activity_order(users, display)
    headers, rows = _activity_table(sessions, display, order, tz, strftime_fmt)
    description = _summary_description(
        count=len(sessions),
        singular_phrase="session was active",
        plural_phrase="sessions were active",
        zero_phrase="No sessions were active",
        start_time=start_time,
        end_time=end_time,
    )

    sections = _summary_sections(summary, start_time, end_time)
    sections.append(_user_totals_section(users, display, order))
    sections.append(
        _table_section(
            TabularSectionSpec(
                heading="User Activity",
                description=description,
                headers=headers,
                rows=rows,
                right_align_columns={headers.index(_ACTIVITY_TIME_HEADER)},
                attachment_filename="activity.csv",
                footnotes=[_DURATION_FOOTNOTE, _timezone_footnote(tz)],
            )
        )
    )

    return Report(title="JupyterHub Activity Report", sections=sections)
