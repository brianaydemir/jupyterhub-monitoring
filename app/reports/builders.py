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
    DocumentTally,
    SessionActivity,
    SkipReason,
    UserActivity,
    aggregate_user_activity,
)
from app.usage.new_users import AdoptionSummary, UserTally, summarize_adoption


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

# The heading of the section each report leads with: what the run
# retrieved, and how much of it could be used, before any figure that
# is only as good as that data.
_COLLECTION_HEADING = "Data Collection"

# How a report's window and its activity figures are read, stated once
# per report on the section that first states the window.
_WINDOW_FOOTNOTE = "The reporting window includes its start and excludes its end."
_SNAPSHOT_FOOTNOTE = (
    "Each snapshot of a running or starting server adds one snapshot interval to active time."
)
_GPU_FOOTNOTE = "A session counts as GPU when one of its spawn options names a GPU."


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


def _format_first_server(ts: int, strftime_fmt: str, tz: tzinfo) -> str:
    """Format a user's first server-start time in the report's zone."""
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

    The new-users tables of a report read one instance, as the activity
    tables of a report do, so no two of them can disagree about a user's
    displayed identifier, about whether the ``Login method`` column is
    shown, or about how an absent domain or login method renders.
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

    A window of no length, or an inverted one, is floored to no time at
    all rather than left as the negative duration it would otherwise
    display as:
    :func:`~app.usage.activity.summarize_activity` already floors such a
    window, but a summary built by hand need not have.
    """
    return Duration(max(summary.window_seconds, 0.0))


def _user_table(
    users: list[dict[str, Any]],
    display: _UserDisplay,
    tz: tzinfo,
    strftime_fmt: str,
    first_server: dict[str, int] | None,
) -> tuple[list[str], list[list[Cell]]]:
    """Build headers and rows for one table of new users.

    A *first_server* of ``None`` drops the "First server" column, as for
    users who have not started a server, rather than filling it with
    ``"n/a"`` for every row. Otherwise every user in *users* must appear
    in it.

    Rows are ordered by creation date, and an equal date breaks on the
    same identity key the activity tables break an equal total on. A row
    is one entry of *users*, not one username, so a user repeated in the
    input is listed as many times as it appears.
    """
    dated = [
        (
            _format_iso_timestamp(user.get("created", ""), strftime_fmt, tz),
            user.get("name", ""),
        )
        for user in users
    ]
    ordered = sorted(dated, key=lambda r: (r[0], display.name_key(r[1])))

    headers = ["Created"]
    if first_server is not None:
        headers.append("First server")
    headers.extend(display.headers())

    rows: list[list[Cell]] = []
    for created, name in ordered:
        row: list[Cell] = [created]
        if first_server is not None:
            row.append(_format_first_server(first_server[name], strftime_fmt, tz))
        row.extend(display.cells(name))
        rows.append(row)
    return headers, rows


def _user_collection_section(
    tally: UserTally, start_time: datetime, end_time: datetime
) -> ReportSection:
    """Build the ``Data Collection`` section of the new-users report.

    The undated count is shown whether or not it is zero, so that a
    reader never has to wonder whether its absence means none or
    unchecked: a user with no creation time is otherwise in no table.
    """
    return _stats_section(
        _COLLECTION_HEADING,
        (
            "Users listed by JupyterHub, and how many were created from "
            f"{start_time.strftime(DATETIME_FMT)} to {end_time.strftime(DATETIME_FMT)}."
        ),
        ["Metric", "Value"],
        [
            ["Users retrieved", tally.retrieved],
            ["Users with no creation time", tally.undated],
            ["Created in window", tally.in_window],
        ],
        footnotes=[_WINDOW_FOOTNOTE],
    )


def _adoption_rows(adoption: AdoptionSummary) -> list[list[Cell]]:
    """Build the ``Metric``/``Value`` rows for the adoption table.

    The population leads, then the split of it, the users who have not
    started a server first, then how quickly the users who did start got
    started. The median row is dropped rather than filled with ``"n/a"``
    when no elapsed time could be measured, since a median over nothing
    is not a figure a reader can act on.
    """
    rows: list[list[Cell]] = [
        ["New users", adoption.new_users],
        ["Never started a server", adoption.never_started],
        ["Started a server", adoption.started],
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


@dataclass(frozen=True)
class _UserTableSpec:
    """What distinguishes one new-users table section from another."""

    heading: str
    description: str
    attachment_filename: str
    first_server: dict[str, int] | None = None


def _user_table_section(
    users: list[dict[str, Any]],
    display: _UserDisplay,
    tz: tzinfo,
    strftime_fmt: str,
    spec: _UserTableSpec,
) -> ReportSection:
    """Build a section holding one table of new users."""
    headers, rows = _user_table(users, display, tz, strftime_fmt, spec.first_server)
    return _table_section(
        TabularSectionSpec(
            heading=spec.heading,
            description=spec.description,
            headers=headers,
            rows=rows,
            attachment_filename=spec.attachment_filename,
            footnotes=[_timezone_footnote(tz)],
        )
    )


def _started_count_sentence(count: int, predicate: str) -> str:
    """Return a sentence counting the new users a table lists."""
    subject = "new user has" if count == 1 else "new users have"
    return f"{count} {subject} {predicate}."


def build_new_users_report(
    users: list[dict[str, Any]],
    tally: UserTally,
    start_time: datetime,
    end_time: datetime,
    strftime_fmt: str,
    detailed_usernames: bool = False,
    first_server: dict[str, int] | None = None,
    anonymize: bool = False,
) -> Report:
    """Build a report object for recently created JupyterHub users.

    A ``Data Collection`` section leads the report, stating from *tally*
    how many users the hub listed and how many of them carried no
    creation time, since the tables below say nothing of either.

    Whenever *first_server* was supplied, an ``Adoption`` section counts
    how many of the new users went on to start a server and how quickly,
    and the users are then split into a ``Not Yet Started`` table and a
    ``Started`` table, in that order, so that the users who have done
    nothing yet are listed before those who have. Both tables read one
    display identity, so they agree about the ``Login method`` column.

    The reporting window supplies the zone: timestamps are rendered in
    *start_time*'s zone, which is also the one the report's footnotes
    name. :func:`~app.usage.new_users.summarize_adoption` reads that
    same zone, so its same-day count agrees with the dates the tables
    show.

    *first_server* maps a username to the epoch-seconds time of that
    user's first server start; users absent from the mapping have never
    started a server. Pass ``None`` when Elasticsearch was not consulted
    at all, which lists every user in a single ``New Users`` table with
    no "First server" column and omits the ``Adoption`` section: with
    nothing to date, every figure in that section would understate users
    who may well have started servers.

    When *anonymize* is set, within-institution identifiers are replaced
    with generic pseudonyms; the institution is still shown.

    Raises:
        ValueError: If *start_time* or *end_time* is not
            timezone-aware.
    """
    tz = _report_zone(start_time, end_time)
    display = _user_display(
        (user.get("name", "") for user in users), detailed_usernames, anonymize
    )

    sections = [_user_collection_section(tally, start_time, end_time)]
    if first_server is None:
        description = _summary_description(
            count=len(users),
            singular_phrase="new user created",
            plural_phrase="new users created",
            zero_phrase="No new users created",
            start_time=start_time,
            end_time=end_time,
        )
        sections.append(
            _user_table_section(
                users,
                display,
                tz,
                strftime_fmt,
                _UserTableSpec("New Users", description, "new-users.csv"),
            )
        )
    else:
        not_started = [user for user in users if user.get("name", "") not in first_server]
        started = [user for user in users if user.get("name", "") in first_server]
        sections.append(_adoption_section(summarize_adoption(users, first_server, tz)))
        sections.append(
            _user_table_section(
                not_started,
                display,
                tz,
                strftime_fmt,
                _UserTableSpec(
                    "Not Yet Started",
                    _started_count_sentence(len(not_started), "not started a server"),
                    "new-users-not-started.csv",
                ),
            )
        )
        sections.append(
            _user_table_section(
                started,
                display,
                tz,
                strftime_fmt,
                _UserTableSpec(
                    "Started",
                    _started_count_sentence(len(started), "started a server"),
                    "new-users-started.csv",
                    first_server=first_server,
                ),
            )
        )

    return Report(title="JupyterHub New Users Report", sections=sections)


def _activity_order(users: list[UserActivity], display: _UserDisplay) -> dict[str, int]:
    """Return the position each username takes in the per-user activity tables.

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


def _session_sort_key(session: SessionActivity) -> tuple[str, str]:
    """Return a sort key ordering a user's started sessions by start time.

    The server name breaks ties, so a user's concurrent named servers
    keep a stable order.
    """
    return (session.started or "", session.server_name or "")


# The header of the duration column in both per-session tables. The
# optional ``Server`` column moves it, so it is found by name.
_ACTIVITY_TIME_HEADER = "Time"

# What a ``Servers Starting`` row adds up: every snapshot of one of a
# user's servers taken while it was spawning, since those carry no start.
_STARTING_FOOTNOTE = (
    "Each row sums one user's server over the snapshots that showed it starting, "
    "before it had a start time."
)


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


@dataclass(frozen=True)
class _SessionColumns:
    """The columns both per-session tables share, after any of their own.

    ``Servers Starting`` and ``User Activity`` each read one instance,
    so the two agree about whether the ``Server`` column is shown and
    about the order the shared columns come in.
    """

    display: _UserDisplay
    show_server: bool

    def headers(self) -> list[str]:
        """Return the shared column headers."""
        headers = ["Server"] if self.show_server else []
        return [*headers, _ACTIVITY_TIME_HEADER, *self.display.headers(), "GPU"]

    def cells(self, session: SessionActivity) -> list[Cell]:
        """Return the shared cells for a session."""
        cells: list[Cell] = [_server_label(session.server_name)] if self.show_server else []
        return [
            *cells,
            Duration(session.seconds),
            *self.display.cells(session.user),
            "Yes" if session.gpu else "No",
        ]


def _starting_section(
    sessions: list[SessionActivity],
    columns: _SessionColumns,
    start_time: datetime,
    end_time: datetime,
) -> ReportSection:
    """Build the ``Servers Starting`` section from the unstarted sessions.

    The longest time spent starting leads, since a server that took
    longest to start, or never did, is the one a reader looks for here.
    An equal time breaks on the displayed identity and then the server.

    A report in which no server was seen starting still gets the
    section, headers and no rows, so that the report's layout does not
    change with what the window happened to hold.
    """
    ordered = sorted(
        sessions,
        key=lambda s: (-s.seconds, columns.display.name_key(s.user), s.server_name or ""),
    )
    headers = columns.headers()
    return _table_section(
        TabularSectionSpec(
            heading="Servers Starting",
            description=_summary_description(
                count=len(sessions),
                singular_phrase="server was seen starting",
                plural_phrase="servers were seen starting",
                zero_phrase="No server was seen starting",
                start_time=start_time,
                end_time=end_time,
            ),
            headers=headers,
            rows=[columns.cells(session) for session in ordered],
            right_align_columns={headers.index(_ACTIVITY_TIME_HEADER)},
            attachment_filename="servers-starting.csv",
            footnotes=[_DURATION_FOOTNOTE, _STARTING_FOOTNOTE],
        )
    )


def _user_activity_section(
    sessions: list[SessionActivity],
    columns: _SessionColumns,
    order: dict[str, int],
    tz: tzinfo,
    strftime_fmt: str,
    window: tuple[datetime, datetime],
) -> ReportSection:
    """Build the ``User Activity`` section from the started sessions.

    Users appear in *order*, the one ``User Totals`` reads, so a heavy
    user's sessions sit where that table puts the user;
    :func:`_session_sort_key` orders the rows within each user.

    ``Server`` follows ``Started``, so that the two columns naming the
    session sit together ahead of the ones naming the user, and is
    present only when :func:`_show_server` finds a named server: without
    it, two of one user's concurrent servers render as identical rows.
    """
    start_time, end_time = window
    sorted_sessions = sorted(sessions, key=lambda s: (order[s.user], _session_sort_key(s)))
    headers = ["Started", *columns.headers()]
    rows: list[list[Cell]] = [
        [
            _format_iso_timestamp(session.started or "", strftime_fmt, tz),
            *columns.cells(session),
        ]
        for session in sorted_sessions
    ]
    return _table_section(
        TabularSectionSpec(
            heading="User Activity",
            description=_summary_description(
                count=len(sessions),
                singular_phrase="started session was active",
                plural_phrase="started sessions were active",
                zero_phrase="No started sessions were active",
                start_time=start_time,
                end_time=end_time,
            ),
            headers=headers,
            rows=rows,
            right_align_columns={headers.index(_ACTIVITY_TIME_HEADER)},
            attachment_filename="activity.csv",
            footnotes=[_DURATION_FOOTNOTE, _timezone_footnote(tz)],
        )
    )


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


def _document_collection_section(
    tally: DocumentTally,
    summary: ActivitySummary,
    start_time: datetime,
    end_time: datetime,
) -> ReportSection:
    """Build the ``Data Collection`` section of the activity report.

    Every skip reason has a row, a zero included, so that a reader never
    has to wonder whether an absent reason means none or unchecked. Its
    notes say how the window is read, since this is the first section to
    state it, and a report drawn from several hubs says so here, since
    every figure after it combines them.

    The description states the window's bounds alone: its length is the
    ``Reporting window`` row of ``Usage``, which is read in the unit
    that section notes.
    """
    skipped: list[list[Cell]] = [
        [f"Skipped: {reason.value}", tally.skipped.get(reason, 0)] for reason in SkipReason
    ]
    rows: list[list[Cell]] = [
        ["Documents retrieved", tally.retrieved],
        ["Documents counted", tally.counted],
        *skipped,
    ]
    footnotes = [_WINDOW_FOOTNOTE]
    if len(summary.hubs) > 1:
        footnotes.append(f"Figures aggregate across hubs: {', '.join(sorted(summary.hubs))}.")
    return _stats_section(
        _COLLECTION_HEADING,
        (
            f"Snapshots taken from {start_time.strftime(DATETIME_FMT)} "
            f"to {end_time.strftime(DATETIME_FMT)}, and how many could be counted."
        ),
        ["Metric", "Value"],
        rows,
        footnotes=footnotes,
    )


def _usage_rows(summary: ActivitySummary) -> list[list[Cell]]:
    """Build the ``Metric``/``Value`` rows for the usage table.

    These are the figures that say whether the hub was used at all. The
    window's length leads, so that a reader meets the scale the
    quantities below it are measured against before the quantities, and
    the longest session sits beside it for comparison.

    A day with no activity is one whose total is not positive, the same
    test that keeps such a day from being named the busiest.
    """
    idle_days = sum(1 for secs in summary.per_day_seconds.values() if secs <= 0)
    return [
        ["Reporting window", _window_length(summary)],
        ["Active users", summary.active_users],
        ["Sessions", summary.total_sessions],
        ["Total active time", Duration(summary.total_seconds)],
        ["Longest session", Duration(summary.max_session_seconds)],
        ["Mean concurrent servers (window)", Fixed(summary.mean_concurrency_window, digits=2)],
        ["Days with no activity", idle_days],
    ]


def _statistics_rows(summary: ActivitySummary) -> list[list[Cell]]:
    """Build the ``Metric``/``Value`` rows for the statistics table.

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


def _aggregate_sections(summary: ActivitySummary) -> list[ReportSection]:
    """Build the sections of aggregate figures, in the order they are read.

    ``Usage`` and the daily series come first, since they say whether
    anything happened at all; ``Statistics`` holds the breakdowns that
    matter only once something has. The two concurrency means are split
    between them, and each section explains its own. A report drawn from
    several hubs notes beside the per-push figures that they alone do
    not combine the hubs, the window mean dividing a total that already
    does.

    The daily section is present whenever the summary holds a day, which
    a summary computed over a window of any length does. What is dropped
    is therefore a window holding no day at all rather than a window that
    saw no activity: the latter is a real, if quiet, series, and a table
    of zeros states that more plainly than an absent section does.
    """
    sections = [
        _stats_section(
            "Usage",
            "Whether and how much the hub was used.",
            ["Metric", "Value"],
            _usage_rows(summary),
            footnotes=[
                _DURATION_FOOTNOTE,
                _SNAPSHOT_FOOTNOTE,
                '"(window)" averages over the whole reporting window, idle stretches included.',
            ],
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
    statistics_footnotes = [
        _DURATION_FOOTNOTE,
        _GPU_FOOTNOTE,
        '"(active)" averages over snapshots that had at least one active server.',
    ]
    if len(summary.hubs) > 1:
        statistics_footnotes.append(
            "Peak and mean (active) concurrency are measured per hub push."
        )
    sections.append(
        _stats_section(
            "Statistics",
            "Usage broken down by GPU, user, session, concurrency, and day.",
            ["Metric", "Value"],
            _statistics_rows(summary),
            footnotes=statistics_footnotes,
        )
    )
    return sections


def build_activity_report(
    sessions: list[SessionActivity],
    summary: ActivitySummary,
    tally: DocumentTally,
    start_time: datetime,
    end_time: datetime,
    strftime_fmt: str,
    detailed_usernames: bool = False,
    anonymize: bool = False,
) -> Report:
    """Build a report object for per-session JupyterHub server activity.

    The sections run from what may be wrong, through whether anything
    happened, to the detail:

    - ``Data Collection``, from *tally*: the documents retrieved, and
      how many could be counted and why the rest could not.
    - ``Servers Starting``: each session that never carried a start
      time, being made of snapshots taken while its server was spawning.
    - ``Usage``, and, when the summary covers at least one day,
      ``Activity by Day``.
    - ``Statistics``: the remaining aggregate figures.
    - ``User Totals``: one row per user, most active first, its sessions
      and time including those in ``Servers Starting``.
    - ``User Activity``: one row per started session, identified by the
      user, the named server, and that server's start time; the ``GPU``
      column reflects whether the session requested a GPU.

    Both per-session tables name the server in a ``Server`` column
    whenever any session ran on a named one, so that two of a user's
    concurrent servers cannot read as one row printed twice; ``User
    Totals`` gains no such column, since it sums a user's servers
    together. ``User Totals`` and ``User Activity`` take their user
    order from one :func:`_activity_order`, so a user found in one is
    found at the same place in the other, that user's sessions ordered
    by start time within it.

    The reporting window supplies the zone: timestamps are rendered in
    *start_time*'s zone, which is also the one the report's footnote
    names.

    *summary* must have been computed over that same window: the
    ``Reporting window`` row reads the window's length from
    ``summary.window_seconds``, while the bounds the descriptions state
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
    columns = _SessionColumns(
        display=_user_display((user.user for user in users), detailed_usernames, anonymize),
        show_server=_show_server(sessions),
    )
    order = _activity_order(users, columns.display)
    started = [session for session in sessions if session.started]
    starting = [session for session in sessions if not session.started]

    sections = [
        _document_collection_section(tally, summary, start_time, end_time),
        _starting_section(starting, columns, start_time, end_time),
        *_aggregate_sections(summary),
        _user_totals_section(users, columns.display, order),
        _user_activity_section(
            started, columns, order, tz, strftime_fmt, (start_time, end_time)
        ),
    ]

    return Report(title="JupyterHub Activity Report", sections=sections)
