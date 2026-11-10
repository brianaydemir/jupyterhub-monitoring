"""Tests for :mod:`jupyterhub_monitoring.reports.builders`."""

# One module per module under test is the layout the suite keeps,
# and builders holds every report table, so this one runs long.
# pylint: disable=too-many-lines

from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from jupyterhub_monitoring.core.time_utils import parse_timezone
from jupyterhub_monitoring.reports.anonymize import build_pseudonyms
from jupyterhub_monitoring.reports.builders import build_activity_report, build_new_users_report
from jupyterhub_monitoring.reports.model import (
    Cell,
    Duration,
    Fixed,
    Percent,
    Report,
    ReportSection,
    TableBlock,
    cell_text,
)
from jupyterhub_monitoring.usage.activity import (
    ActivitySummary,
    DocumentTally,
    SessionActivity,
    SkipReason,
    summarize_activity,
)
from jupyterhub_monitoring.usage.new_users import UserTally
from jupyterhub_monitoring.usage.pod_metrics import PodMetrics

CHICAGO = ZoneInfo("America/Chicago")
START = datetime(2026, 7, 1, tzinfo=CHICAGO)
END = datetime(2026, 7, 8, tzinfo=CHICAGO)

# Two identifiers at one institution whose pseudonyms collide,
# found by search over the 26 ** 3 label space.
COLLIDING_UIDS = ("u138", "u223")

# Two identifiers at one institution
# whose pseudonyms sort the opposite way round from the raw uids,
# so a table ordered on the raw value shows their pseudonyms
# in descending order.
INVERTING_UIDS = ("alice", "bob")

# The footnote every section showing a duration carries.
TIMES_NOTE = "Times are HH:MM."


def test_new_users_report_shows_created_and_the_identity_columns() -> None:
    """Without Elasticsearch, the report is Created plus the identity columns."""
    table = _users_table(_users_report([_user("a@x.edu", "2026-07-02T10:00:00Z")]))
    assert table.headers == ["Created", "Institution", "ID"]
    assert table.rows == [["2026-07-02", "x.edu", "a"]]


def test_new_users_report_adds_a_first_server_column_when_given_one() -> None:
    """Supplying the mapping dates the users who started a server."""
    users = [_user("a@x.edu", "2026-07-02T10:00:00Z")]
    report = _users_report(users, first_server={"a@x.edu": 1782000000})
    assert _table(report, "Started").headers == ["Created", "First server", "Institution", "ID"]
    assert _table(report, "Not Yet Started").headers == ["Created", "Institution", "ID"]


def test_new_users_are_listed_oldest_account_first() -> None:
    """The roster reads in creation order, whatever order the hub listed it in."""
    report = _users_report(
        [_user("a@x.edu", "2026-07-03T10:00:00Z"), _user("b@x.edu", "2026-07-02T10:00:00Z")]
    )
    assert [row[2] for row in _users_table(report).rows] == ["b", "a"]


def test_new_users_report_lists_users_with_no_server_apart() -> None:
    """A user absent from the mapping is listed as not yet started."""
    report = _users_report([_user("a@x.edu", "2026-07-02T10:00:00Z")], first_server={})
    assert _table(report, "Not Yet Started").rows == [["2026-07-02", "x.edu", "a"]]
    assert _table(report, "Started").rows == []


def test_new_users_report_runs_collection_then_adoption_then_users() -> None:
    """What may be wrong comes first, then who has done nothing yet."""
    report = _users_report(
        [_user("a@x.edu", "2026-07-02T10:00:00Z"), _user("b@x.edu", "2026-07-02T10:00:00Z")],
        first_server={"a@x.edu": _epoch("2026-07-02T10:30:00Z")},
    )
    assert [section.heading for section in report.sections] == [
        "Data Collection",
        "Adoption",
        "Not Yet Started",
        "Started",
    ]


def test_no_adoption_section_without_elasticsearch() -> None:
    """Without first-server times, every figure in it would understate."""
    report = _users_report([_user("a@x.edu", "2026-07-02T10:00:00Z")], first_server=None)
    assert [section.heading for section in report.sections] == ["Data Collection", "New Users"]


def test_new_users_collection_states_the_tally() -> None:
    """The rows are the tally's, an undated count of zero included."""
    report = _users_report([], tally=UserTally(retrieved=812, undated=0, in_window=0))
    assert _values(report, "Data Collection") == {
        "Users retrieved": 812,
        "Users with no creation time": 0,
        "Created in window": 0,
    }
    assert _section(report, "Data Collection").description == (
        "Users listed by JupyterHub, and how many were created from "
        "2026-07-01 00:00 to 2026-07-08 00:00."
    )


def test_the_adoption_section_survives_nobody_starting_a_server() -> None:
    """An empty mapping means Elasticsearch was consulted and found none."""
    rows = _adoption(_users_report([_user("a@x.edu", "2026-07-02T10:00:00Z")], first_server={}))
    assert rows["Started a server"] == 0
    assert rows["Never started a server"] == 1
    assert "Median time to first server" not in rows


def test_adoption_counts_split_the_users_the_table_lists() -> None:
    """The two outcomes are exhaustive, over the rows the reader can count."""
    report = _users_report(
        [
            _user("a@x.edu", "2026-07-02T10:00:00Z"),
            _user("b@x.edu", "2026-07-02T10:00:00Z"),
            _user("c@x.edu", "2026-07-02T10:00:00Z"),
        ],
        first_server={"a@x.edu": _epoch("2026-07-02T10:30:00Z")},
    )
    rows = _adoption(report)
    assert rows["New users"] == 3
    assert rows["Never started a server"] == len(_table(report, "Not Yet Started").rows) == 2
    assert rows["Started a server"] == len(_table(report, "Started").rows) == 1


def test_the_two_new_users_tables_share_one_login_method_decision() -> None:
    """Two users colliding across the tables still need disambiguating."""
    report = _users_report(
        [
            _user("a@x.edu", "2026-07-02T10:00:00Z"),
            _user("eppn:a@x.edu", "2026-07-02T11:00:00Z"),
        ],
        first_server={"a@x.edu": _epoch("2026-07-02T10:30:00Z")},
    )
    assert _table(report, "Not Yet Started").rows == [
        ["2026-07-02", "x.edu", "a", "legacy eppn"]
    ]
    assert _table(report, "Started").headers[-1] == "Login method"


def test_the_new_users_tables_count_their_rows() -> None:
    """Each table says how many users it lists."""
    report = _users_report(
        [
            _user("a@x.edu", "2026-07-02T10:00:00Z"),
            _user("b@x.edu", "2026-07-02T10:00:00Z"),
        ],
        first_server={"a@x.edu": _epoch("2026-07-02T10:30:00Z")},
    )
    assert _section(report, "Not Yet Started").description == (
        "1 new user has not started a server."
    )
    assert _section(report, "Started").description == "1 new user has started a server."
    empty = _users_report([], first_server={})
    assert _section(empty, "Not Yet Started").description == (
        "0 new users have not started a server."
    )


def test_same_day_starts_are_counted_in_the_report_timezone() -> None:
    """The count agrees with the two dates the table puts side by side.

    Read in UTC these two instants fall on different days,
    so a count taken there would contradict a table showing one date twice.
    """
    users = [_user("a@x.edu", "2026-07-01T20:00:00Z")]
    first_server = {"a@x.edu": _epoch("2026-07-02T02:00:00Z")}

    chicago = _users_report(users, first_server=first_server)
    created, first = _table(chicago, "Started").rows[0][:2]
    assert created == first == "2026-07-01"
    assert _adoption(chicago)["Started same day"] == 1

    utc_start, utc_end = datetime(2026, 7, 1, tzinfo=UTC), datetime(2026, 7, 8, tzinfo=UTC)
    in_utc = build_new_users_report(
        users=users,
        tally=UserTally(),
        start_time=utc_start,
        end_time=utc_end,
        strftime_fmt="%Y-%m-%d",
        first_server=first_server,
    )
    assert _adoption(in_utc)["Started same day"] == 0


def test_the_median_time_to_first_server_is_a_duration_with_its_unit_noted() -> None:
    """The report's one duration names the units it is read in."""
    report = _users_report(
        [
            _user("a@x.edu", "2026-07-02T10:00:00Z"),
            _user("b@x.edu", "2026-07-02T10:00:00Z"),
        ],
        first_server={
            "a@x.edu": _epoch("2026-07-02T10:30:00Z"),
            "b@x.edu": _epoch("2026-07-02T11:30:00Z"),
        },
    )
    assert _adoption(report)["Median time to first server"] == Duration(3600)
    assert _section(report, "Adoption").footnotes == [TIMES_NOTE]


def test_new_users_report_shows_login_method_when_ids_collide() -> None:
    """Two login methods for one (Institution, ID) need disambiguating."""
    table = _users_table(
        _users_report(
            [
                _user("a@x.edu", "2026-07-02T10:00:00Z"),
                _user("eppn:a@x.edu", "2026-07-02T11:00:00Z"),
            ]
        )
    )
    assert table.headers == ["Created", "Institution", "ID", "Login method"]
    assert [row[-1] for row in table.rows] == ["local NetID", "legacy eppn"]


def test_new_users_report_hides_login_method_when_ids_are_distinct() -> None:
    """The column only appears when it is needed."""
    table = _users_table(
        _users_report(
            [
                _user("a@x.edu", "2026-07-02T10:00:00Z"),
                _user("b@x.edu", "2026-07-02T11:00:00Z"),
            ]
        )
    )
    assert "Login method" not in table.headers


def test_detailed_usernames_forces_the_login_method_column() -> None:
    """The flag shows the column even with nothing to disambiguate."""
    table = _users_table(
        _users_report([_user("a@x.edu", "2026-07-02T10:00:00Z")], detailed_usernames=True)
    )
    assert table.headers[-1] == "Login method"


def test_a_repeated_username_does_not_force_the_login_method_column() -> None:
    """One user listed twice is still one identity, not an ambiguity."""
    table = _users_table(
        _users_report(
            [
                _user("a@x.edu", "2026-07-02T10:00:00Z"),
                _user("a@x.edu", "2026-07-02T10:00:00Z"),
            ]
        )
    )
    assert "Login method" not in table.headers


def test_new_users_report_marks_a_name_carrying_no_institution() -> None:
    """A blank cell would read as data the report failed to fill in.

    The name's login method is absent for the same reason
    and is marked the same way; the recognized name beside it is untouched.
    """
    table = _users_table(
        _users_report(
            [
                _user("service-account", "2026-07-02T10:00:00Z"),
                _user("a@x.edu", "2026-07-02T10:00:00Z"),
            ],
            detailed_usernames=True,
        )
    )
    assert table.headers == ["Created", "Institution", "ID", "Login method"]
    assert table.rows == [
        ["2026-07-02", "x.edu", "a", "local NetID"],
        ["2026-07-02", "(none)", "service-account", "(none)"],
    ]


@pytest.mark.parametrize("detailed_usernames", [False, True])
def test_new_users_report_sorts_an_unrecognized_name_last(detailed_usernames: bool) -> None:
    """It is the least informative row, and the two orderings agree on that.

    The table orders on the parse priority when the ``Login method`` column
    is shown and on the domain's trailing rank when it is not;
    a name holding neither sorts after even ORCID either way.
    """
    users = [
        _user("service-account", "2026-07-02T10:00:00Z"),
        _user("orcid:0009-0008-3064-0494", "2026-07-02T10:00:00Z"),
        _user("a@x.edu", "2026-07-02T10:00:00Z"),
    ]
    table = _users_table(_users_report(users, detailed_usernames=detailed_usernames))
    assert [row[2] for row in table.rows] == [
        "a",
        "0009-0008-3064-0494",
        "service-account",
    ]


def test_anonymize_replaces_ids_but_keeps_institutions() -> None:
    """The institution is still shown; the identifier is not."""
    table = _users_table(
        _users_report([_user("alice@x.edu", "2026-07-02T10:00:00Z")], anonymize=True)
    )
    pseudonym = table.rows[0][2]
    assert isinstance(pseudonym, str)
    assert table.rows[0][1] == "x.edu"
    assert pseudonym != "alice"
    assert len(pseudonym.split(" ")) == 3


def test_anonymize_shows_login_method_when_pseudonyms_collide() -> None:
    """Ambiguity is judged on what the reader sees, not the raw identifiers.

    These two users have distinct raw identifiers,
    so a check against the raw pairs would hide the column
    while the displayed rows are identical.
    """
    first, second = COLLIDING_UIDS
    table = _users_table(
        _users_report(
            [
                _user(f"{first}@x.edu", "2026-07-02T10:00:00Z"),
                _user(f"{second}@x.edu", "2026-07-02T11:00:00Z"),
            ],
            anonymize=True,
        )
    )
    assert table.headers[-1] == "Login method"
    assert table.rows[0][2] == table.rows[1][2]


def test_an_unrecognized_names_pseudonym_is_keyed_on_its_empty_domain() -> None:
    """The marker is rendered, never parsed, so the pseudonym is unchanged.

    Keying on the marker instead would relabel every unrecognized name
    and break the cross-run stability the pseudonyms exist to provide,
    so the label is pinned here
    rather than recomputed from the code under test.
    """
    table = _users_table(
        _users_report([_user("service-account", "2026-07-02T10:00:00Z")], anonymize=True)
    )
    assert table.rows[0][1:3] == ["(none)", "Romeo Tango Papa"]
    assert build_pseudonyms([("", "service-account")]) == {
        ("", "service-account"): "Romeo Tango Papa"
    }


def test_new_users_report_describes_the_window() -> None:
    """The count and the window are stated above the table."""
    section = _section(_users_report([_user("a@x.edu", "2026-07-02T10:00:00Z")]), "New Users")
    assert section.description == (
        "1 new user created from 2026-07-01 00:00 to 2026-07-08 00:00."
    )
    assert _section(_users_report([]), "New Users").description == (
        "No new users created between 2026-07-01 00:00 and 2026-07-08 00:00."
    )
    two = _users_report(
        [
            _user("a@x.edu", "2026-07-02T10:00:00Z"),
            _user("b@x.edu", "2026-07-02T10:00:00Z"),
        ]
    )
    assert "2 new users created" in _section(two, "New Users").description


@pytest.mark.parametrize("tz_str", ["America/Chicago", "+04:00"])
def test_reports_name_the_zone_of_their_own_window(tz_str: str) -> None:
    """The footnote is read off the window, so the two cannot disagree."""
    tz = parse_timezone(tz_str)
    start, end = datetime(2026, 7, 1, tzinfo=tz), datetime(2026, 7, 8, tzinfo=tz)
    users = build_new_users_report(
        users=[], tally=UserTally(), start_time=start, end_time=end, strftime_fmt="%Y-%m-%d"
    )
    activity = build_activity_report(
        sessions=[],
        summary=ActivitySummary(),
        tally=DocumentTally(),
        start_time=start,
        end_time=end,
        strftime_fmt="%Y-%m-%d %H:%M",
    )
    assert f"Timezone: {tz_str}" in users.sections[-1].footnotes
    assert f"Timezone: {tz_str}" in activity.sections[-1].footnotes


def test_created_timestamps_are_shown_in_the_report_timezone() -> None:
    """A UTC instant is displayed as the local time it corresponds to."""
    table = _users_table(
        _users_report([_user("a@x.edu", "2026-07-02T02:00:00Z")], date_format="datetime")
    )
    assert table.rows[0][0] == "2026-07-01 21:00"


def test_an_unparsable_created_timestamp_is_passed_through() -> None:
    """A surprising value is visible in the report, not silently dropped."""
    table = _users_table(_users_report([_user("a@x.edu", "sometime last week")]))
    assert table.rows[0][0] == "sometime last week"


@pytest.mark.parametrize("field", ["start", "end"])
def test_builders_reject_a_naive_window(field: str) -> None:
    """Every timestamp in the report is rendered in a known zone."""
    naive = datetime(2026, 7, 1)
    with pytest.raises(ValueError, match=f"{field}_time must be timezone-aware"):
        build_new_users_report(
            users=[],
            tally=UserTally(),
            start_time=naive if field == "start" else START,
            end_time=naive if field == "end" else END,
            strftime_fmt="%Y-%m-%d",
        )
    with pytest.raises(ValueError, match=f"{field}_time must be timezone-aware"):
        build_activity_report(
            sessions=[],
            summary=ActivitySummary(),
            tally=DocumentTally(),
            start_time=naive if field == "start" else START,
            end_time=naive if field == "end" else END,
            strftime_fmt="%Y-%m-%d %H:%M",
        )


def test_activity_report_sections_run_from_problems_to_detail() -> None:
    """What may be wrong, then whether anything happened, then the detail."""
    report = _activity_report(
        sessions=[_session("a@x.edu", seconds=3600)],
        summary=ActivitySummary(total_seconds=3600, per_day_seconds={"2026-07-02": 3600}),
    )
    assert [s.heading for s in report.sections] == [
        "Data Collection",
        "Servers Starting",
        "Usage",
        "Activity by Day",
        "Statistics",
        "User Totals",
        "User Activity",
    ]


def test_activity_by_day_is_omitted_without_daily_data() -> None:
    """An empty per-day mapping means there is no daily table to show."""
    report = _activity_report(sessions=[], summary=ActivitySummary())
    assert [s.heading for s in report.sections] == [
        "Data Collection",
        "Servers Starting",
        "Usage",
        "Statistics",
        "User Totals",
        "User Activity",
    ]


def test_collection_rows_name_every_skip_reason() -> None:
    """A reason that did not occur reads as zero rather than vanishing."""
    tally = DocumentTally(retrieved=10, counted=7, skipped={SkipReason.NO_INTERVAL: 3})
    report = _activity_report([], ActivitySummary(), tally=tally)
    assert _values(report, "Data Collection") == {
        "Documents retrieved": 10,
        "Documents counted": 7,
        "Skipped: no user.name": 0,
        "Skipped: no meta.interval": 3,
        "Skipped: unparsable meta.interval": 0,
        "Skipped: non-positive meta.interval": 0,
    }


def test_the_aggregate_tables_state_each_summary_figure() -> None:
    """Every row reads its own field, so two figures cannot be swapped unseen."""
    summary = ActivitySummary(
        window_seconds=7 * 86400,
        total_seconds=40 * 3600,
        gpu_seconds=10 * 3600,
        active_users=5,
        total_sessions=12,
        gpu_users=2,
        gpu_sessions=3,
        mean_user_seconds=8 * 3600,
        median_user_seconds=6 * 3600,
        max_user_seconds=20 * 3600,
        max_session_seconds=9 * 3600,
        mean_sessions_per_user=2.4,
        mean_session_seconds=12000,
        peak_concurrency=4,
        mean_concurrency_active=1.75,
        mean_concurrency_window=0.24,
        per_day_seconds={"2026-07-02": 3600, "2026-07-03": 7200, "2026-07-04": 0.0},
    )
    report = _activity_report([], summary)
    assert _values(report, "Usage") == {
        "Reporting window": Duration(7 * 86400),
        "Active users": 5,
        "Sessions": 12,
        "Total active time": Duration(40 * 3600),
        "Longest session": Duration(9 * 3600),
        "Mean concurrent servers (window)": Fixed(0.24, digits=2),
        "Days with no activity": 1,
    }
    assert _values(report, "Statistics") == {
        "GPU active time": Duration(10 * 3600),
        "GPU share of total": Percent(0.25),
        "Non-GPU active time": Duration(30 * 3600),
        "GPU users": 2,
        "GPU sessions": 3,
        "Mean time per user": Duration(8 * 3600),
        "Median time per user": Duration(6 * 3600),
        "Max time (one user)": Duration(20 * 3600),
        "Mean sessions per user": Fixed(2.4, digits=1),
        "Mean session length": Duration(12000),
        "Peak concurrent servers": 4,
        "Mean concurrent servers (active)": Fixed(1.75, digits=2),
        "Busiest day": "2026-07-03",
        "Busiest day active time": Duration(7200),
    }


def test_collection_description_states_the_window() -> None:
    """The first section states the window's bounds.

    The session table's own sentence is a count of the sessions it lists.
    """
    report = _activity_report(
        [_session("a@x.edu")], ActivitySummary(window_seconds=7 * 24 * 3600)
    )
    assert _section(report, "Data Collection").description == (
        "Snapshots taken from 2026-07-01 00:00 to 2026-07-08 00:00, "
        "and how many could be counted."
    )
    assert _section(report, "User Activity").description == (
        "1 started session was active from 2026-07-01 00:00 to 2026-07-08 00:00."
    )


@pytest.mark.parametrize("gpu_seconds", [0.0, 3600.0])
def test_the_gpu_share_of_an_idle_window_is_zero(gpu_seconds: float) -> None:
    """A total of no time is not divided by; the share reads 0%, not n/a."""
    summary = ActivitySummary(total_seconds=0.0, gpu_seconds=gpu_seconds)
    values = _values(_activity_report([], summary), "Statistics")
    assert values["GPU share of total"] == Percent(0.0)


@pytest.mark.parametrize(
    "per_day_seconds",
    [dict[str, float](), {"2026-07-02": 0.0, "2026-07-03": 0.0}],
    ids=["no-days", "quiet-days"],
)
def test_no_busiest_day_is_named_unless_a_day_saw_activity(
    per_day_seconds: dict[str, float],
) -> None:
    """A window that saw nothing has no busiest day, only zeros."""
    summary = ActivitySummary(per_day_seconds=per_day_seconds)
    values = _values(_activity_report([], summary), "Statistics")
    assert values["Busiest day"] == "n/a"
    assert values["Busiest day active time"] == "n/a"


def test_footnotes_name_the_hubs_a_report_combines() -> None:
    """Figures from several hubs are ambiguous unless the report says so."""
    report = _activity_report([], ActivitySummary(hubs={"b", "a"}))
    assert _section(report, "Data Collection").footnotes[-1] == (
        "Figures aggregate across hubs: a, b."
    )
    assert _section(report, "Statistics").footnotes[-1] == (
        "Peak and mean (active) concurrency are measured per hub push."
    )


@pytest.mark.parametrize("hubs", [set[str](), {"a"}])
def test_one_hub_or_none_adds_no_footnote(hubs: set[str]) -> None:
    """The common case is unambiguous, so it stays uncluttered."""
    report = _activity_report([], ActivitySummary(hubs=hubs))
    assert not [
        note for section in report.sections for note in section.footnotes if "hub" in note
    ]


def test_every_section_showing_a_duration_states_its_unit() -> None:
    """The unit is a note on the table, not a suffix on one header.

    A section showing no duration gains no such note,
    since it would explain nothing:
    the adoption section has one only when it shows a median.
    """
    users = [_user("a@x.edu", "2026-07-02T10:00:00Z")]
    reports = [
        _activity_report(
            [
                _session("a@x.edu", seconds=3600),
                _session("a@x.edu", started=None, seconds=60),
            ],
            ActivitySummary(per_day_seconds={"2026-07-02": 3600}),
        ),
        _users_report(users),
        _users_report(users, first_server={}),
        _users_report(users, first_server={"a@x.edu": _epoch("2026-07-02T10:30:00Z")}),
    ]
    for report in reports:
        for section in report.sections:
            shows_duration = any(
                isinstance(cell, Duration)
                for block in section.blocks
                if isinstance(block, TableBlock)
                for row in block.rows
                for cell in row
            )
            assert (TIMES_NOTE in section.footnotes) == shows_duration, section.heading


def test_activity_by_day_is_sorted_by_day() -> None:
    """Days read in calendar order, not in document order."""
    summary = ActivitySummary(per_day_seconds={"2026-07-03": 7200, "2026-07-02": 3600})
    assert _table(_activity_report([], summary), "Activity by Day").rows == [
        ["2026-07-02", Duration(3600)],
        ["2026-07-03", Duration(7200)],
    ]


def test_activity_by_day_still_lists_a_window_that_saw_no_activity() -> None:
    """A table of zeros states the quiet window; an absent section would not."""
    summary = ActivitySummary(per_day_seconds={"2026-07-02": 0.0, "2026-07-03": 0.0})
    assert _table(_activity_report([], summary), "Activity by Day").rows == [
        ["2026-07-02", Duration(0.0)],
        ["2026-07-03", Duration(0.0)],
    ]


def test_user_totals_sum_each_users_sessions() -> None:
    """One row per user, with the counts and durations right-aligned."""
    sessions = [
        _session("a@x.edu", started="2026-07-02T10:00:00Z", seconds=4800, gpu=True),
        _session("a@x.edu", started="2026-07-02T13:00:00Z", seconds=3000),
    ]
    table = _totals_table(_activity_report(sessions, ActivitySummary()))
    assert table.headers == ["Institution", "ID", "Sessions", "Time", "GPU time"]
    assert table.rows == [["x.edu", "a", 2, Duration(7800), Duration(4800)]]
    assert table.right_align_columns == {2, 3, 4}


def test_user_totals_are_ordered_by_time_most_active_first() -> None:
    """The section answers who is consuming the resources."""
    sessions = [
        _session("a@x.edu", seconds=60),
        _session("b@x.edu", seconds=3600),
        _session("c@x.edu", seconds=600),
    ]
    table = _totals_table(_activity_report(sessions, ActivitySummary()))
    assert [row[1] for row in table.rows] == ["b", "c", "a"]


def test_user_totals_break_an_equal_time_on_the_displayed_identity() -> None:
    """Equal totals order as the per-session table orders their users."""
    sessions = [
        _session("b@x.edu", seconds=3600),
        _session("a@x.edu", seconds=3600),
        _session("orcid:0009-0008-3064-0494", seconds=3600),
    ]
    table = _totals_table(_activity_report(sessions, ActivitySummary()))
    assert [row[1] for row in table.rows] == ["a", "b", "0009-0008-3064-0494"]
    assert [row[0] for row in table.rows] == ["x.edu", "x.edu", "orcid.org"]


def test_user_totals_order_by_the_pseudonym_not_the_raw_identifier() -> None:
    """Ordering an anonymized table on the raw uid would leak it."""
    first, second = INVERTING_UIDS
    sessions = [
        _session(f"{first}@x.edu", seconds=3600),
        _session(f"{second}@x.edu", seconds=3600),
    ]
    labels = build_pseudonyms([("x.edu", first), ("x.edu", second)])
    plain = _totals_table(_activity_report(sessions, ActivitySummary()))
    anonymized = _totals_table(_activity_report(sessions, ActivitySummary(), anonymize=True))
    assert [row[1] for row in plain.rows] == [first, second]
    assert [row[1] for row in anonymized.rows] == [
        labels[("x.edu", second)],
        labels[("x.edu", first)],
    ]


def test_user_totals_show_login_method_when_the_sessions_do() -> None:
    """Both tables judge the ambiguity from the same displayed pairs."""
    sessions = [
        _session("a@x.edu", seconds=3600),
        _session("eppn:a@x.edu", seconds=60),
    ]
    report = _activity_report(sessions, ActivitySummary())
    assert _totals_table(report).headers == [
        "Institution",
        "ID",
        "Login method",
        "Sessions",
        "Time",
        "GPU time",
    ]
    assert "Login method" in _sessions_table(report).headers
    assert _totals_table(report).rows[0][2] == "local NetID"


def test_user_totals_hide_login_method_when_the_sessions_do() -> None:
    """Distinct displayed pairs need no disambiguating column, in either table."""
    report = _activity_report(
        [_session("a@x.edu", seconds=3600), _session("b@x.edu", seconds=60)],
        ActivitySummary(),
    )
    assert "Login method" not in _totals_table(report).headers
    assert "Login method" not in _sessions_table(report).headers


def test_a_spawn_only_session_counts_toward_a_users_total() -> None:
    """The count matches the rows the two per-session tables show together."""
    sessions = [
        _session("a@x.edu", started=None, seconds=60),
        _session("a@x.edu", started="2026-07-02T10:00:00Z", seconds=3600),
    ]
    report = _activity_report(sessions, ActivitySummary())
    assert _totals_table(report).rows == [["x.edu", "a", 2, Duration(3660), Duration(0.0)]]
    assert len(_sessions_table(report).rows) == len(_starting_table(report).rows) == 1


def test_user_totals_of_an_idle_window_are_headers_alone() -> None:
    """An empty table reads as no user being active, as the sessions do."""
    report = _activity_report([], ActivitySummary())
    table = _totals_table(report)
    assert table.headers == ["Institution", "ID", "Sessions", "Time", "GPU time"]
    assert table.rows == []
    assert _sessions_table(report).rows == []
    assert _starting_table(report).rows == []


def test_user_totals_agree_with_the_summary() -> None:
    """The table and the aggregate sections are two views of one aggregation."""
    sessions = [
        _session("a@x.edu", started="2026-07-02T10:00:00Z", seconds=4800, gpu=True),
        _session("a@x.edu", started="2026-07-02T13:00:00Z", seconds=3000),
        _session("b@x.edu", started="2026-07-02T10:00:00Z", seconds=5400, gpu=True),
    ]
    report = _activity_report(sessions, summarize_activity(sessions, [], CHICAGO, START, END))
    rows = _totals_table(report).rows
    usage = _values(report, "Usage")
    statistics = _values(report, "Statistics")
    assert len(rows) == usage["Active users"]
    assert sum(_count(row[2]) for row in rows) == usage["Sessions"]
    assert Duration(sum(_seconds(row[3]) for row in rows)) == usage["Total active time"]
    assert rows[0][3] == statistics["Max time (one user)"]
    assert statistics["GPU active time"] == Duration(sum(_seconds(row[4]) for row in rows))


def test_user_activity_lists_one_row_per_session() -> None:
    """One row per session, with the GPU flag last."""
    table = _sessions_table(
        _activity_report([_session("a@x.edu", seconds=3600, gpu=True)], ActivitySummary())
    )
    assert table.headers == ["Started", "Time", "Institution", "ID", "GPU"]
    assert table.rows == [["2026-07-02 05:00", Duration(3600), "x.edu", "a", "Yes"]]


def test_concurrent_named_servers_render_as_distinguishable_rows() -> None:
    """Without the server name the two rows would be byte-identical."""
    sessions = [
        _session("a@x.edu", started="2026-07-02T10:00:00Z", seconds=600, server="gpu"),
        _session("a@x.edu", started="2026-07-02T10:00:00Z", seconds=600, server="cpu"),
    ]
    table = _sessions_table(_activity_report(sessions, ActivitySummary()))
    assert table.headers == [
        "Started",
        "Server",
        "Time",
        "Institution",
        "ID",
        "GPU",
    ]
    assert [row[1] for row in table.rows] == ["cpu", "gpu"]
    assert table.rows[0] != table.rows[1]


def test_a_server_name_the_document_omitted_reads_as_not_available() -> None:
    """``None`` is a document that did not say, not the default server."""
    sessions = [
        _session("a@x.edu", started="2026-07-02T10:00:00Z", seconds=600, server="gpu"),
        _session("a@x.edu", started="2026-07-02T11:00:00Z", seconds=600, server=None),
        _session("a@x.edu", started="2026-07-02T12:00:00Z", seconds=600),
    ]
    table = _sessions_table(_activity_report(sessions, ActivitySummary()))
    assert [row[1] for row in table.rows] == ["gpu", "n/a", "(default)"]


@pytest.mark.parametrize("server", ["", "gpu"])
def test_the_per_session_tables_right_align_their_duration(server: str) -> None:
    """The index has to follow the column, which ``Server`` shifts."""
    sessions = [
        _session("a@x.edu", seconds=3600, server=server),
        _session("a@x.edu", started=None, seconds=60, server=server),
    ]
    report = _activity_report(sessions, ActivitySummary())
    assert _aligned(_sessions_table(report)) == ["Time"]
    assert _aligned(_starting_table(report)) == ["Time"]


def test_pod_metrics_add_columns_to_the_started_sessions() -> None:
    """A session Prometheus could not measure reads n/a, not zero.

    Each figure is a number rather than text, so the workbook can sort it,
    and its unit is stated once, in the table's notes.
    """
    measured = _session("a@x.edu", seconds=3600)
    unmeasured = _session("b@x.edu", seconds=60)
    report = _activity_report(
        [measured, unmeasured],
        ActivitySummary(),
        pod_metrics=PodMetrics(
            cpu_time={measured.key: 5400.0}, peak_memory={measured.key: 1.5 * 2**30}
        ),
    )
    table = _sessions_table(report)
    assert table.headers == [
        "Started",
        "Time",
        "Institution",
        "ID",
        "GPU",
        "CPU time",
        "Max memory",
    ]
    assert [row[-2:] for row in table.rows] == [
        [Duration(5400.0), Fixed(1.5, digits=2)],
        ["n/a", "n/a"],
    ]
    assert _aligned(table) == ["Time", "CPU time", "Max memory"]
    assert any("GiB" in note for note in _section(report, "User Activity").footnotes)


def test_user_totals_never_gain_a_server_column() -> None:
    """The section sums a user's servers, so naming one would mislead."""
    sessions = [
        _session("a@x.edu", started="2026-07-02T10:00:00Z", seconds=600, server="gpu"),
        _session("a@x.edu", started="2026-07-02T10:00:00Z", seconds=600, server="cpu"),
    ]
    table = _totals_table(_activity_report(sessions, ActivitySummary()))
    assert "Server" not in table.headers
    assert table.rows == [["x.edu", "a", 2, Duration(1200), Duration(0.0)]]


def test_a_session_that_never_started_is_listed_as_starting() -> None:
    """Snapshots taken while spawning carry no ``server.started``."""
    report = _activity_report(
        [_session("a@x.edu", started=None, seconds=60, gpu=True)], ActivitySummary()
    )
    starting = _starting_table(report)
    assert starting.headers == ["Time", "Institution", "ID", "GPU"]
    assert starting.rows == [[Duration(60), "x.edu", "a", "Yes"]]
    assert _sessions_table(report).rows == []


def test_servers_starting_is_present_when_no_server_was() -> None:
    """An empty table keeps the report's layout the same from run to run."""
    report = _activity_report([_session("a@x.edu", seconds=60)], ActivitySummary())
    assert _starting_table(report).rows == []
    assert _section(report, "Servers Starting").description == (
        "No server was seen starting between 2026-07-01 00:00 and 2026-07-08 00:00."
    )


def test_servers_starting_lists_the_longest_time_first() -> None:
    """An equal time breaks on the displayed identity, then the server."""
    sessions = [
        _session("a@x.edu", started=None, seconds=60, server="b"),
        _session("b@x.edu", started=None, seconds=600),
        _session("a@x.edu", started=None, seconds=60, server="a"),
        _session("c@x.edu", started=None, seconds=60),
    ]
    table = _starting_table(_activity_report(sessions, ActivitySummary()))
    assert [(row[3], row[0]) for row in table.rows] == [
        ("b", "(default)"),
        ("a", "a"),
        ("a", "b"),
        ("c", "(default)"),
    ]


def test_the_per_session_tables_share_one_server_column_decision() -> None:
    """A named server in either table puts the column in both."""
    sessions = [
        _session("a@x.edu", started=None, seconds=60),
        _session("a@x.edu", seconds=3600, server="gpu"),
    ]
    report = _activity_report(sessions, ActivitySummary())
    assert _starting_table(report).rows == [["(default)", Duration(60), "x.edu", "a", "No"]]
    assert _sessions_table(report).headers[1] == "Server"


def test_the_activity_tables_mark_a_name_carrying_no_institution() -> None:
    """One ``_UserDisplay`` feeds both tables, so both show the marker.

    The absent login method is marked the same way,
    and the recognized user beside it is untouched.
    """
    sessions = [
        _session("service-account", seconds=600),
        _session("a@x.edu", seconds=3600),
    ]
    report = _activity_report(sessions, ActivitySummary(), detailed_usernames=True)
    assert _totals_table(report).rows == [
        ["x.edu", "a", "local NetID", 1, Duration(3600), Duration(0.0)],
        ["(none)", "service-account", "(none)", 1, Duration(600), Duration(0.0)],
    ]
    assert [row[2:5] for row in _sessions_table(report).rows] == [
        ["x.edu", "a", "local NetID"],
        ["(none)", "service-account", "(none)"],
    ]


def test_the_two_user_tables_list_their_users_in_one_order() -> None:
    """A user sits in the same place in both, which is the point of the pair."""
    sessions = [
        _session("a@x.edu", started="2026-07-02T10:00:00Z", seconds=3600),
        _session("b@x.edu", started="2026-07-02T11:00:00Z", seconds=1800),
        _session("b@x.edu", started="2026-07-02T12:00:00Z", seconds=1800),
        _session("c@x.edu", started="2026-07-02T13:00:00Z", seconds=3600),
        _session("orcid:0009-0008-3064-0494", started="2026-07-02T14:00:00Z", seconds=600),
    ]
    report = _activity_report(sessions, ActivitySummary())
    totals = [row[1] for row in _totals_table(report).rows]
    sessions_ids = [row[3] for row in _sessions_table(report).rows]
    assert totals == ["a", "b", "c", "0009-0008-3064-0494"]
    assert list(dict.fromkeys(sessions_ids)) == totals


def test_a_users_own_sessions_keep_their_start_order() -> None:
    """Within a user: start time, and the server name on a tie.

    The named servers put a ``Server`` column in this table,
    so the identity and duration columns sit one place further right.
    """
    sessions = [
        _session("z@x.edu", started="2026-07-02T09:00:00Z", seconds=7200),
        _session("a@x.edu", started="2026-07-02T12:00:00Z", seconds=120, server="b"),
        _session("a@x.edu", started="2026-07-02T12:00:00Z", seconds=60, server="a"),
        _session("a@x.edu", started="2026-07-02T10:00:00Z", seconds=60),
    ]
    table = _sessions_table(_activity_report(sessions, ActivitySummary()))
    assert [(row[4], row[0], row[2]) for row in table.rows] == [
        ("z", "2026-07-02 04:00", Duration(7200)),
        ("a", "2026-07-02 05:00", Duration(60)),
        ("a", "2026-07-02 07:00", Duration(60)),
        ("a", "2026-07-02 07:00", Duration(120)),
    ]


def test_activity_report_attaches_its_user_tables() -> None:
    """The ``Metric``/``Value`` and per-day tables are not attached separately.

    The user tables carry distinct filenames,
    since :func:`~jupyterhub_monitoring.reports.renderers.report_attachments`
    de-duplicates by filename and would drop one of them.
    """
    report = _activity_report(
        [_session("a@x.edu", seconds=60)],
        ActivitySummary(per_day_seconds={"2026-07-02": 60}),
    )
    assert _attachment_filenames(report) == [
        None,
        "servers-starting.csv",
        None,
        None,
        None,
        "user-totals.csv",
        "activity.csv",
    ]


@pytest.mark.parametrize(
    ("first_server", "expected"),
    [
        (None, [None, "new-users.csv"]),
        ({}, [None, None, "new-users-not-started.csv", "new-users-started.csv"]),
    ],
)
def test_new_users_report_attaches_its_user_tables(
    first_server: dict[str, int] | None, expected: list[str | None]
) -> None:
    """The user tables are the report's data, so they are the attachments."""
    assert _attachment_filenames(_users_report([], first_server=first_server)) == expected


def _epoch(text: str) -> int:
    """Return an ISO 8601 instant as the epoch seconds a mapping holds."""
    return int(datetime.fromisoformat(text).timestamp())


def _adoption(report: Report) -> dict[str, Cell]:
    """Return the ``Adoption`` table as a metric-to-value mapping."""
    return _values(report, "Adoption")


def _values(report: Report, heading: str) -> dict[str, Cell]:
    """Return a ``Metric``/``Value`` table as a mapping of its two columns."""
    return {cell_text(row[0]): row[1] for row in _table(report, heading).rows}


def _attachment_filenames(report: Report) -> list[str | None]:
    """Return every table's attachment filename, in report order."""
    return [
        block.attachment_filename
        for section in report.sections
        for block in section.blocks
        if isinstance(block, TableBlock)
    ]


def _aligned(table: TableBlock) -> list[str]:
    """Return the headers of the table's right-aligned columns, in order."""
    return [table.headers[idx] for idx in sorted(table.right_align_columns)]


def _count(value: Cell) -> int:
    """Return a count cell's value, which the tables carry as an ``int``."""
    assert isinstance(value, int)
    return value


def _seconds(value: Cell) -> float:
    """Return a duration cell's seconds, which the tables carry typed."""
    assert isinstance(value, Duration)
    return value.seconds


def _user(name: str, created: str) -> dict[str, Any]:
    """Build the user record shape ``filter_new_users`` produces."""
    return {"name": name, "created": created}


def _session(
    user: str,
    *,
    started: str | None = "2026-07-02T10:00:00Z",
    seconds: float = 0.0,
    gpu: bool = False,
    server: str | None = "",
) -> SessionActivity:
    """Build a session, on the default server unless *server* names one.

    A *server* of ``None`` stands for a document that named no server.
    """
    return SessionActivity(
        user=user, server_name=server, started=started, seconds=seconds, gpu=gpu
    )


def _users_report(
    users: list[dict[str, Any]],
    *,
    tally: UserTally | None = None,
    first_server: dict[str, int] | None = None,
    detailed_usernames: bool = False,
    anonymize: bool = False,
    date_format: str = "date",
) -> Report:
    """Build a new-users report over the standard window.

    The tally defaults to one in which every user listed is new.
    """
    return build_new_users_report(
        users=users,
        tally=tally or UserTally(retrieved=len(users), in_window=len(users)),
        start_time=START,
        end_time=END,
        strftime_fmt="%Y-%m-%d" if date_format == "date" else "%Y-%m-%d %H:%M",
        detailed_usernames=detailed_usernames,
        first_server=first_server,
        anonymize=anonymize,
    )


def _activity_report(
    sessions: list[SessionActivity],
    summary: ActivitySummary,
    *,
    tally: DocumentTally | None = None,
    detailed_usernames: bool = False,
    anonymize: bool = False,
    pod_metrics: PodMetrics | None = None,
) -> Report:
    """Build an activity report over the standard window."""
    return build_activity_report(
        sessions=sessions,
        summary=summary,
        tally=tally or DocumentTally(),
        start_time=START,
        end_time=END,
        strftime_fmt="%Y-%m-%d %H:%M",
        detailed_usernames=detailed_usernames,
        anonymize=anonymize,
        pod_metrics=pod_metrics,
    )


def _totals_table(report: Report) -> TableBlock:
    """Return the ``User Totals`` table."""
    return _table(report, "User Totals")


def _sessions_table(report: Report) -> TableBlock:
    """Return the ``User Activity`` table of started sessions."""
    return _table(report, "User Activity")


def _starting_table(report: Report) -> TableBlock:
    """Return the ``Servers Starting`` table of unstarted sessions."""
    return _table(report, "Servers Starting")


def _users_table(report: Report) -> TableBlock:
    """Return the ``New Users`` table of a report made without Elasticsearch."""
    return _table(report, "New Users")


def _section(report: Report, heading: str) -> ReportSection:
    """Return *report*'s one section headed *heading*."""
    sections = [section for section in report.sections if section.heading == heading]
    assert len(sections) == 1
    return sections[0]


def _table(report: Report, heading: str) -> TableBlock:
    """Return the single table block of *report*'s section headed *heading*."""
    blocks = [b for b in _section(report, heading).blocks if isinstance(b, TableBlock)]
    assert len(blocks) == 1
    return blocks[0]
