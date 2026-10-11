"""Tests for :mod:`app.usage.activity`."""

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from app.usage.activity import (
    ActivitySummary,
    DocumentTally,
    SkipReason,
    UserActivity,
    aggregate_user_activity,
    compute_activity,
    summarize_activity,
    tally_documents,
)

CHICAGO = ZoneInfo("America/Chicago")

# The reporting window the tests summarize over: the local day that the
# default snapshot falls on, so that the days it fills are the one day
# under test. A test whose subject is the window itself passes its own
# bounds.
START = datetime(2026, 6, 20, tzinfo=CHICAGO)
END = datetime(2026, 6, 21, tzinfo=CHICAGO)


def test_activity_sums_the_interval_of_each_snapshot() -> None:
    """Each countable snapshot adds its own push interval."""
    sessions = compute_activity([_doc(), _doc(), _doc()])
    assert len(sessions) == 1
    assert sessions[0].seconds == 900.0


def test_activity_keys_sessions_by_user_server_and_start() -> None:
    """Concurrent named servers and separate runs are tallied apart."""
    sessions = compute_activity(
        [
            _doc(server="", started="2026-07-02T10:00:00Z"),
            _doc(server="gpu", started="2026-07-02T10:00:00Z"),
            _doc(server="", started="2026-07-02T12:00:00Z"),
            _doc(user="b@x.edu", started="2026-07-02T10:00:00Z"),
        ]
    )
    assert len(sessions) == 4
    assert {s.seconds for s in sessions} == {300.0}


def test_spawn_attempts_on_one_server_share_a_session() -> None:
    """Snapshots taken while spawning carry no start time to key on."""
    sessions = compute_activity([_doc(started=None), _doc(started=None)])
    assert len(sessions) == 1
    assert sessions[0].started is None
    assert sessions[0].seconds == 600.0


def test_a_session_is_gpu_if_any_of_its_snapshots_is() -> None:
    """A GPU request anywhere in the run marks the whole session."""
    sessions = compute_activity([_doc(), _doc(options={"server.user_options.gpus": "1"})])
    assert sessions[0].gpu is True


@pytest.mark.parametrize(
    "options",
    [
        {"server.user_options.gpus": "1"},
        {"server.user_options.gpu": True},
        {"server.user_options.gpu": "yes"},
        {"server.user_options.resources": "gpu"},
        {"server.user_options.profile": "GPU node"},
        {"server.user_options.profile": "GPU (A100)"},
        {"server.user_options.profile": ["gpu", "large"]},
        {"server.user_options": ["cpu", "gpu"]},
        {"server.user_options.resources": "cpu_and_gpu"},
        {"server.user_options.profile": "Large GPU node"},
        # An accepted limit of the word-anywhere rule, not an oversight:
        # a GPU-capable image, or a note that mentions a GPU, is
        # indistinguishable from a request for one.
        {"server.user_options.image": "tensorflow-gpu:latest"},
        {
            "server.user_options.image": "tensorflow-gpu:latest",
            "server.user_options.gpus": "0",
        },
        {"server.user_options.notes": "no gpu needed"},
    ],
)
def test_gpu_requests_are_recognized(options: dict[str, Any]) -> None:
    """A GPU is named by an option's key, or by a value that contains it."""
    assert compute_activity([_doc(options=options)])[0].gpu is True


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"server.user_options.gpu": False},
        {"server.user_options.gpus": False},
        {"server.user_options.gpu": "false"},
        {"server.user_options.gpu": "0"},
        {"server.user_options.gpus": "0"},
        {"server.user_options.gpu": "off"},
        {"server.user_options.gpu": ""},
        {"server.user_options.cpus": "4"},
        {"server.user_options.profile": "gpumesh"},
        {"server.user_options.profile": "nogpu"},
        {"server.user_options.resources": "cpu_and_gpumesh"},
    ],
)
def test_non_gpu_requests_are_not_billed_as_gpu(options: dict[str, Any]) -> None:
    """An unchecked box, and a value lacking the whole word, do not count."""
    assert compute_activity([_doc(options=options)])[0].gpu is False


@pytest.mark.parametrize(
    "overrides",
    [
        {"user.name": None},
        {"user.name": ""},
        {"meta.interval": None},
        {"meta.interval": ""},
        {"meta.interval": "not-a-duration"},
        {"meta.interval": 300},
        {"meta.interval": "0"},
        {"meta.interval": "-5m"},
    ],
)
def test_documents_without_a_user_or_interval_are_skipped(
    overrides: dict[str, Any],
) -> None:
    """A snapshot that cannot be attributed or measured contributes nothing."""
    doc = _doc() | overrides
    assert compute_activity([doc]) == []
    assert summarize_activity([], [doc], CHICAGO, START, END) == ActivitySummary(
        window_seconds=(END - START).total_seconds(),
        per_day_seconds={"2026-06-20": 0.0},
    )


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"user.name": ""}, SkipReason.NO_USER),
        ({"user.name": 42}, SkipReason.NO_USER),
        ({"meta.interval": ""}, SkipReason.NO_INTERVAL),
        ({"meta.interval": 300}, SkipReason.NO_INTERVAL),
        ({"meta.interval": "not-a-duration"}, SkipReason.BAD_INTERVAL),
        ({"meta.interval": "0"}, SkipReason.NON_POSITIVE_INTERVAL),
        ({"meta.interval": "-5m"}, SkipReason.NON_POSITIVE_INTERVAL),
    ],
)
def test_each_skip_is_counted_under_its_own_reason(
    overrides: dict[str, Any],
    reason: SkipReason,
) -> None:
    """The reasons are distinct, since only they tell the failures apart."""
    tally = tally_documents([_doc() | overrides])
    assert tally == DocumentTally(retrieved=1, counted=0, skipped={reason: 1})


def test_a_mixed_document_set_counts_each_reason_separately() -> None:
    """Counted and skipped documents always sum back to those retrieved."""
    docs = [
        _doc(),
        _doc(user="b@x.edu"),
        _doc() | {"user.name": ""},
        _doc() | {"meta.interval": 300},
        _doc() | {"meta.interval": "not-a-duration"},
        _doc() | {"meta.interval": "not-a-duration"},
        _doc() | {"meta.interval": "-5m"},
    ]
    tally = tally_documents(docs)
    assert tally == DocumentTally(
        retrieved=7,
        counted=2,
        skipped={
            SkipReason.NO_USER: 1,
            SkipReason.NO_INTERVAL: 1,
            SkipReason.BAD_INTERVAL: 2,
            SkipReason.NON_POSITIVE_INTERVAL: 1,
        },
    )
    assert tally.counted + sum(tally.skipped.values()) == tally.retrieved


def test_no_documents_tally_as_none_retrieved_rather_than_skipped() -> None:
    """An idle window retrieves nothing; nothing was thrown away."""
    assert tally_documents([]) == DocumentTally()


def test_user_activity_sums_each_users_sessions() -> None:
    """One row per user, holding the session count and the GPU share."""
    docs = [
        _doc(started="2026-07-02T10:00:00Z", options={"server.user_options.gpu": True}),
        _doc(started="2026-07-02T12:00:00Z"),
        _doc(user="b@x.edu"),
    ]
    users = aggregate_user_activity(compute_activity(docs))
    assert users == [
        UserActivity(user="a@x.edu", sessions=2, seconds=600.0, gpu_seconds=300.0),
        UserActivity(user="b@x.edu", sessions=1, seconds=300.0, gpu_seconds=0.0),
    ]


def test_a_spawn_only_session_counts_toward_its_user() -> None:
    """A session that never started is still one of the user's sessions."""
    docs = [_doc(started=None), _doc(started="2026-07-02T10:00:00Z")]
    (user,) = aggregate_user_activity(compute_activity(docs))
    assert (user.sessions, user.seconds) == (2, 600.0)


def test_users_keep_the_order_they_first_appear_in() -> None:
    """The order a report shows users in is the report's own decision."""
    docs = [_doc(user="b@x.edu"), _doc(user="a@x.edu"), _doc(user="b@x.edu")]
    assert [u.user for u in aggregate_user_activity(compute_activity(docs))] == [
        "b@x.edu",
        "a@x.edu",
    ]


def test_no_sessions_aggregate_to_no_users() -> None:
    """An idle window has no user to total up."""
    assert aggregate_user_activity([]) == []


def test_user_totals_agree_with_the_summary() -> None:
    """The summary's per-user figures are the aggregation's own."""
    docs = [
        _doc(started="2026-07-02T10:00:00Z", options={"server.user_options.gpu": True}),
        _doc(started="2026-07-02T12:00:00Z"),
        _doc(user="b@x.edu"),
    ]
    sessions = compute_activity(docs)
    users = aggregate_user_activity(sessions)
    summary = summarize_activity(sessions, docs, CHICAGO, START, END)
    assert len(users) == summary.active_users
    assert sum(u.sessions for u in users) == summary.total_sessions
    assert sum(u.seconds for u in users) == summary.total_seconds
    assert sum(u.gpu_seconds for u in users) == summary.gpu_seconds
    assert max(u.seconds for u in users) == summary.max_user_seconds


def test_summary_derives_user_statistics_from_sessions() -> None:
    """Per-user figures aggregate the sessions, not the raw documents."""
    docs = [
        _doc(started="2026-07-02T10:00:00Z"),
        _doc(started="2026-07-02T12:00:00Z"),
        _doc(user="b@x.edu"),
    ]
    summary = summarize_activity(compute_activity(docs), docs, CHICAGO, START, END)
    assert summary.active_users == 2
    assert summary.total_sessions == 3
    assert summary.total_seconds == 900.0
    assert summary.mean_user_seconds == 450.0
    assert summary.median_user_seconds == 450.0
    assert summary.max_user_seconds == 600.0
    assert summary.mean_sessions_per_user == 1.5
    assert summary.mean_session_seconds == 300.0


def test_the_longest_session_is_one_session_not_one_user() -> None:
    """A user's two sessions are not added together for this figure."""
    docs = [
        _doc(started="2026-07-02T10:00:00Z"),
        _doc(started="2026-07-02T10:00:00Z"),
        _doc(started="2026-07-02T12:00:00Z"),
    ]
    summary = summarize_activity(compute_activity(docs), docs, CHICAGO, START, END)
    assert summary.max_user_seconds == 900.0
    assert summary.max_session_seconds == 600.0


def test_no_sessions_have_no_longest_session() -> None:
    """An idle window reads as zero, as the other maxima do."""
    assert summarize_activity([], [], CHICAGO, START, END).max_session_seconds == 0.0


def test_summary_counts_gpu_users_and_sessions_separately() -> None:
    """One user with one GPU session among two is one GPU user."""
    docs = [
        _doc(started="2026-07-02T10:00:00Z", options={"server.user_options.gpu": True}),
        _doc(started="2026-07-02T12:00:00Z"),
    ]
    summary = summarize_activity(compute_activity(docs), docs, CHICAGO, START, END)
    assert (summary.gpu_users, summary.gpu_sessions) == (1, 1)
    assert summary.gpu_seconds == 300.0


def test_concurrency_counts_documents_sharing_a_snapshot_time() -> None:
    """Peak concurrency is the busiest snapshot; the mean skips idle ones."""
    docs = [
        _doc(snapshot=1000),
        _doc(snapshot=1000, user="b@x.edu"),
        _doc(snapshot=1000, user="c@x.edu"),
        _doc(snapshot=2000),
    ]
    summary = summarize_activity(compute_activity(docs), docs, CHICAGO, START, END)
    assert summary.peak_concurrency == 3
    assert summary.mean_concurrency_active == 2.0


def test_concurrency_keeps_two_hubs_apart_at_one_snapshot_time() -> None:
    """Hubs whose pushes coincide must not read as one busier hub.

    Grouping on the hub as well as the time is what makes the figures
    the same whether or not two schedules happen to fire in the same
    second.
    """
    docs = [
        _doc(snapshot=1000, hub="a"),
        _doc(snapshot=1000, hub="a", user="b@x.edu"),
        _doc(snapshot=1000, hub="b", user="c@x.edu"),
    ]
    summary = summarize_activity(compute_activity(docs), docs, CHICAGO, START, END)
    assert summary.peak_concurrency == 2
    assert summary.mean_concurrency_active == 1.5


def test_concurrency_groups_the_documents_naming_no_hub_together() -> None:
    """A single-hub index that never sets the field is one group."""
    docs = [
        _doc(snapshot=1000),
        _doc(snapshot=1000, user="b@x.edu"),
        _doc(snapshot=1000, hub="a", user="c@x.edu"),
    ]
    summary = summarize_activity(compute_activity(docs), docs, CHICAGO, START, END)
    assert summary.peak_concurrency == 2
    assert summary.mean_concurrency_active == 1.5


def test_concurrency_is_zero_without_countable_documents() -> None:
    """An empty window reports no servers rather than dividing by zero."""
    summary = summarize_activity([], [], CHICAGO, START, END)
    assert (summary.peak_concurrency, summary.mean_concurrency_active) == (0, 0.0)


def test_the_window_mean_counts_the_idle_part_of_the_window() -> None:
    """One busy hour in a day is a light day (about 0.042), not a busy push."""
    docs = [_doc(interval="1h")]
    summary = summarize_activity(compute_activity(docs), docs, CHICAGO, START, END)
    assert summary.mean_concurrency_active == 1.0
    assert summary.mean_concurrency_window == pytest.approx(1 / 24)


def test_the_two_means_agree_when_the_window_is_never_idle() -> None:
    """With every push busy there is no idle time for the two to differ over."""
    docs = [_doc(snapshot=1782000000 + 300 * i) for i in range(12)]
    summary = summarize_activity(
        compute_activity(docs), docs, CHICAGO, START, START + timedelta(hours=1)
    )
    assert summary.mean_concurrency_window == summary.mean_concurrency_active == 1.0


def test_a_window_of_no_length_is_not_divided_by() -> None:
    """The commands reject such a window, but a direct caller need not."""
    docs = [_doc()]
    summary = summarize_activity(compute_activity(docs), docs, CHICAGO, START, START)
    assert summary.mean_concurrency_window == 0.0


@pytest.mark.parametrize(
    ("window", "expected"),
    [(timedelta(hours=24), 86400.0), (timedelta(days=7), 604800.0), (timedelta(0), 0.0)],
)
def test_the_summary_carries_the_length_of_its_own_window(
    window: timedelta, expected: float
) -> None:
    """The report states the scale its totals are to be read against."""
    summary = summarize_activity([], [], CHICAGO, START, START + window)
    assert summary.window_seconds == expected


def test_the_window_length_is_elapsed_time_across_dst() -> None:
    """Two local days around spring-forward are 47 hours, not 48.

    The durations in the table are summed push intervals, so the length
    they are read against, and the mean taken over it, measure time the
    same way.
    """
    start = datetime(2026, 3, 7, tzinfo=CHICAGO)
    end = datetime(2026, 3, 9, tzinfo=CHICAGO)
    docs = [_doc(interval="1h")]
    summary = summarize_activity(compute_activity(docs), docs, CHICAGO, start, end)
    assert summary.window_seconds == 47 * 3600
    assert summary.mean_concurrency_window == pytest.approx(1 / 47)


def test_an_inverted_window_is_carried_as_no_length() -> None:
    """A summary must not hold a window its own figures deny.

    The window mean already reads such a window as no window, so the
    length it is measured against reads the same way.
    """
    summary = summarize_activity([], [], CHICAGO, START, START - timedelta(hours=1))
    assert summary.window_seconds == 0.0
    assert summary.mean_concurrency_window == 0.0


@pytest.mark.parametrize(
    ("hubs", "expected"),
    [
        ([None, None], set[str]()),
        (["a", "a"], {"a"}),
        (["a", "b", None], {"a", "b"}),
    ],
)
def test_the_summary_records_the_hubs_it_observed(
    hubs: list[str | None], expected: set[str]
) -> None:
    """The hubs are what lets a report say its figures combine them."""
    docs = [_doc(hub=hub, user=f"u{i}@x.edu") for i, hub in enumerate(hubs)]
    assert (
        summarize_activity(compute_activity(docs), docs, CHICAGO, START, END).hubs == expected
    )


def test_a_hub_that_is_not_a_string_is_recorded_as_text() -> None:
    """``meta.hub`` is read defensively, since a mapping need not be text."""
    docs = [_doc(hub=7)]
    assert summarize_activity(compute_activity(docs), docs, CHICAGO, START, END).hubs == {"7"}


def test_a_skipped_document_contributes_no_hub() -> None:
    """A document that no figure counts does not widen the report's scope."""
    docs = [_doc(hub="a"), {"user.name": "b@x.edu", "meta.hub": "b"}]
    assert summarize_activity(compute_activity(docs), docs, CHICAGO, START, END).hubs == {"a"}


def test_per_day_buckets_use_the_report_timezone() -> None:
    """A snapshot near UTC midnight belongs to the local day."""
    utc_midnight = int(datetime(2026, 7, 3, 2, 0, tzinfo=timezone.utc).timestamp())
    summary = summarize_activity(
        [], [_doc(snapshot=utc_midnight)], CHICAGO, _midnight(7, 2), _midnight(7, 3)
    )
    assert summary.per_day_seconds == {"2026-07-02": 300.0}


def test_per_day_sums_every_snapshot_on_a_day() -> None:
    """Daily totals add up the intervals credited to that day."""
    day = int(datetime(2026, 7, 3, 12, 0, tzinfo=timezone.utc).timestamp())
    summary = summarize_activity(
        [],
        [_doc(snapshot=day), _doc(snapshot=day + 300)],
        CHICAGO,
        _midnight(7, 3),
        _midnight(7, 4),
    )
    assert summary.per_day_seconds == {"2026-07-03": 600.0}


def test_per_day_fills_the_days_that_saw_no_activity() -> None:
    """A quiet day holds the series together instead of dropping out of it."""
    day = int(datetime(2026, 7, 2, 12, 0, tzinfo=timezone.utc).timestamp())
    summary = summarize_activity(
        [],
        [_doc(snapshot=day), _doc(snapshot=day + 2 * 86400)],
        CHICAGO,
        _midnight(7, 2),
        _midnight(7, 5),
    )
    assert summary.per_day_seconds == {
        "2026-07-02": 300.0,
        "2026-07-03": 0.0,
        "2026-07-04": 300.0,
    }


def test_a_single_day_window_covers_only_that_day() -> None:
    """The window is half-open, so its end's own day is not part of it."""
    summary = summarize_activity([], [], CHICAGO, _midnight(7, 2), _midnight(7, 3))
    assert summary.per_day_seconds == {"2026-07-02": 0.0}


def test_a_window_ending_mid_day_covers_that_day() -> None:
    """An end past midnight is an instant the report could have observed."""
    end = _midnight(7, 3) + timedelta(minutes=1)
    summary = summarize_activity([], [], CHICAGO, _midnight(7, 2), end)
    assert list(summary.per_day_seconds) == ["2026-07-02", "2026-07-03"]


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        (
            datetime(2026, 10, 30, tzinfo=CHICAGO),
            datetime(2026, 11, 3, tzinfo=CHICAGO),
            ["2026-10-30", "2026-10-31", "2026-11-01", "2026-11-02"],
        ),
        (
            datetime(2026, 3, 7, tzinfo=CHICAGO),
            datetime(2026, 3, 10, tzinfo=CHICAGO),
            ["2026-03-07", "2026-03-08", "2026-03-09"],
        ),
    ],
    ids=["fall-back", "spring-forward"],
)
def test_per_day_walks_calendar_days_across_a_dst_transition(
    start: datetime, end: datetime, expected: list[str]
) -> None:
    """A 23- or 25-hour day is one day, so none is skipped or repeated."""
    summary = summarize_activity([], [], CHICAGO, start, end)
    assert list(summary.per_day_seconds) == expected


@pytest.mark.parametrize("length", [timedelta(0), timedelta(hours=-1)])
def test_a_window_holding_no_instant_covers_no_day(length: timedelta) -> None:
    """A window of no length, or an inverted one, has no day to report.

    Both read as no window everywhere else in the summary, so neither
    gets a row of its own here.
    """
    start = _midnight(7, 2)
    assert not summarize_activity([], [], CHICAGO, start, start + length).per_day_seconds


def test_a_snapshot_outside_the_window_keeps_its_own_day() -> None:
    """The query returns no such document, but a direct caller may pass one."""
    day = int(datetime(2026, 7, 9, 12, 0, tzinfo=timezone.utc).timestamp())
    summary = summarize_activity(
        [], [_doc(snapshot=day)], CHICAGO, _midnight(7, 2), _midnight(7, 3)
    )
    assert summary.per_day_seconds == {"2026-07-02": 0.0, "2026-07-09": 300.0}


def _midnight(month: int, day: int) -> datetime:
    """Return the Chicago midnight that starts the given day of 2026."""
    return datetime(2026, month, day, tzinfo=CHICAGO)


def _doc(
    *,
    user: str = "a@x.edu",
    server: str = "",
    started: str | None = "2026-07-02T10:00:00Z",
    snapshot: int = 1782000000,
    interval: str = "5m",
    hub: Any = None,
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a flattened server snapshot of the shape ``push-servers`` writes."""
    doc: dict[str, Any] = {
        "user.name": user,
        "server.name": server,
        "server.ready": True,
        "meta.snapshot-time": snapshot,
        "meta.interval": interval,
    }
    if started is not None:
        doc["server.started"] = started
    if hub is not None:
        doc["meta.hub"] = hub
    doc.update(options or {})
    return doc
