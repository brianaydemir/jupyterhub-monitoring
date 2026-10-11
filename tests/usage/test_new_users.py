"""Tests for :mod:`app.usage.new_users`."""

# An explicit comparison to an empty collection is the assertion these
# tests mean to make, and reads better here than a truthiness check.
# pylint: disable=use-implicit-booleaness-not-comparison

from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from app.usage.new_users import (
    AdoptionSummary,
    UserTally,
    compute_first_server,
    filter_new_users,
    summarize_adoption,
    tally_users,
)

START = datetime(2026, 7, 1, tzinfo=timezone.utc)
END = datetime(2026, 7, 8, tzinfo=timezone.utc)
CHICAGO = ZoneInfo("America/Chicago")


def test_users_outside_the_window_are_dropped() -> None:
    """Accounts older or newer than the window are not new users here."""
    users = [
        {"name": "old", "created": "2026-06-30T23:59:59Z"},
        {"name": "future", "created": "2026-07-08T00:00:01Z"},
    ]
    assert filter_new_users(users, START, END) == []


def test_the_window_includes_its_start_and_excludes_its_end() -> None:
    """Half-open, so back-to-back reports do not both count a boundary user."""
    users = [
        {"name": "at-start", "created": "2026-07-01T00:00:00Z"},
        {"name": "at-end", "created": "2026-07-08T00:00:00Z"},
    ]
    assert [u["name"] for u in filter_new_users(users, START, END)] == ["at-start"]


def test_a_naive_created_timestamp_is_read_as_utc() -> None:
    """JupyterHub reports UTC; the host's local zone is irrelevant."""
    users = [{"name": "a@x.edu", "created": "2026-07-04T10:00:00"}]
    assert len(filter_new_users(users, START, END)) == 1


def test_an_offset_created_timestamp_is_honored() -> None:
    """A timestamp carrying an offset is compared at its real instant."""
    users = [{"name": "just-before", "created": "2026-07-01T01:00:00+02:00"}]
    assert filter_new_users(users, START, END) == []


@pytest.mark.parametrize(
    "user",
    [
        {"name": "a@x.edu"},
        {"name": "a@x.edu", "created": None},
        {"name": "a@x.edu", "created": ""},
        {"name": "a@x.edu", "created": "last Tuesday"},
        {"name": "a@x.edu", "created": 1782000000},
    ],
)
def test_users_without_a_usable_created_time_are_skipped(user: dict[str, Any]) -> None:
    """A record that cannot be placed in time cannot be placed in the window.

    The tally is what counts it instead.
    """
    assert filter_new_users([user], START, END) == []
    assert tally_users([user], START, END) == UserTally(retrieved=1, undated=1, in_window=0)


def test_the_tally_counts_the_users_the_filter_keeps() -> None:
    """One rule decides both, so the tally and the selection agree."""
    users: list[dict[str, Any]] = [
        {"name": "at-start", "created": "2026-07-01T00:00:00Z"},
        {"name": "at-end", "created": "2026-07-08T00:00:00Z"},
        {"name": "old", "created": "2026-06-30T23:59:59Z"},
        {"name": "undated", "created": None},
        {"name": "garbled", "created": "last Tuesday"},
    ]
    tally = tally_users(users, START, END)
    assert tally == UserTally(retrieved=5, undated=2, in_window=1)
    assert tally.in_window == len(filter_new_users(users, START, END))


def test_no_users_tally_as_none_retrieved() -> None:
    """A hub that answered with nothing is told apart from a quiet window."""
    assert tally_users([], START, END) == UserTally()


def test_filtered_users_are_reduced_to_name_and_created() -> None:
    """Only the two fields the report needs are carried forward."""
    users: list[dict[str, Any]] = [
        {
            "name": "a@x.edu",
            "created": "2026-07-04T10:00:00Z",
            "admin": True,
            "servers": {},
        }
    ]
    assert filter_new_users(users, START, END) == [
        {"name": "a@x.edu", "created": "2026-07-04T10:00:00Z"}
    ]


def test_first_server_uses_the_earliest_start_time() -> None:
    """The first start is the earliest ``server.started`` seen for the user."""
    docs = [
        _doc(started="2026-07-04T10:00:00Z"),
        _doc(started="2026-07-02T09:00:00Z"),
        _doc(started="2026-07-06T11:00:00Z"),
    ]
    expected = int(datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc).timestamp())
    assert compute_first_server(docs) == {"a@x.edu": expected}


def test_first_server_falls_back_to_the_snapshot_time() -> None:
    """A snapshot taken while spawning carries no start time of its own."""
    assert compute_first_server([_doc(started=None, snapshot=1782000000)]) == {
        "a@x.edu": 1782000000
    }


def test_first_server_prefers_a_start_time_over_a_snapshot_time() -> None:
    """The recorded start is exact; the snapshot time is an upper bound."""
    started = "2026-07-02T09:00:00Z"
    expected = int(datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc).timestamp())
    assert compute_first_server([_doc(started=started, snapshot=1782000000)]) == {
        "a@x.edu": expected
    }


def test_first_server_tracks_each_user_separately() -> None:
    """One entry per user, not per document."""
    docs = [
        _doc(user="a@x.edu", started="2026-07-02T09:00:00Z"),
        _doc(user="b@x.edu", started="2026-07-03T09:00:00Z"),
    ]
    assert set(compute_first_server(docs)) == {"a@x.edu", "b@x.edu"}


@pytest.mark.parametrize(
    "overrides",
    [
        {"user.name": None},
        {"user.name": ""},
        {"user.name": 42},
    ],
)
def test_first_server_skips_documents_without_a_user(
    overrides: dict[str, Any],
) -> None:
    """A start time with nobody to attribute it to is unusable."""
    assert compute_first_server([_doc() | overrides]) == {}


def test_first_server_skips_documents_with_no_usable_time() -> None:
    """Neither a start time nor a snapshot time means nothing to record."""
    doc = _doc(started=None)
    del doc["meta.snapshot-time"]
    assert compute_first_server([doc]) == {}


def test_first_server_ignores_an_unparsable_start_time() -> None:
    """A malformed start falls back to the snapshot time."""
    doc = _doc(started="whenever", snapshot=1782000000)
    assert compute_first_server([doc]) == {"a@x.edu": 1782000000}


def test_adoption_over_no_users_says_nothing_happened() -> None:
    """Every count is zero, and there is no median to report."""
    assert summarize_adoption([], {}, timezone.utc) == AdoptionSummary()


def test_adoption_splits_the_new_users_into_starters_and_the_rest() -> None:
    """The two outcomes are exhaustive: they sum back to the roster."""
    users = [_new("a@x.edu"), _new("b@x.edu"), _new("c@x.edu")]
    summary = summarize_adoption(
        users, {"a@x.edu": _epoch("2026-07-04T11:00:00Z")}, timezone.utc
    )
    assert (summary.new_users, summary.started, summary.never_started) == (3, 1, 2)
    assert summary.started + summary.never_started == summary.new_users


def test_adoption_counts_a_repeated_user_as_the_table_lists_them() -> None:
    """Counting per record is what keeps the split summing to the rows."""
    users = [_new("a@x.edu"), _new("a@x.edu")]
    summary = summarize_adoption(
        users, {"a@x.edu": _epoch("2026-07-04T11:00:00Z")}, timezone.utc
    )
    assert (summary.new_users, summary.started, summary.never_started) == (2, 2, 0)


def test_same_day_starts_are_judged_in_the_given_zone() -> None:
    """The zone decides the answer, so it must be the one the report renders."""
    users = [_new("a@x.edu", "2026-07-01T20:00:00Z")]
    first_server = {"a@x.edu": _epoch("2026-07-02T02:00:00Z")}
    assert summarize_adoption(users, first_server, CHICAGO).same_day == 1
    assert summarize_adoption(users, first_server, timezone.utc).same_day == 0


def test_a_user_who_never_started_is_not_a_same_day_starter() -> None:
    """Only a dated first server can share a day with its user's creation."""
    assert summarize_adoption([_new("a@x.edu")], {}, timezone.utc).same_day == 0


def test_the_median_averages_the_middle_pair_of_an_even_number_of_users() -> None:
    """Ten and thirty minutes, ninety minutes, and four hours."""
    users = [_new(name) for name in ("a@x.edu", "b@x.edu", "c@x.edu", "d@x.edu")]
    first_server = {
        "a@x.edu": _epoch("2026-07-04T10:10:00Z"),
        "b@x.edu": _epoch("2026-07-04T10:30:00Z"),
        "c@x.edu": _epoch("2026-07-04T11:30:00Z"),
        "d@x.edu": _epoch("2026-07-04T14:00:00Z"),
    }
    summary = summarize_adoption(users, first_server, timezone.utc)
    assert summary.median_seconds_to_first == (1800 + 5400) / 2


def test_no_median_is_reported_when_nobody_started_a_server() -> None:
    """A median over an empty set is not a figure a reader can act on."""
    summary = summarize_adoption([_new("a@x.edu")], {}, timezone.utc)
    assert summary.started == 0
    assert summary.median_seconds_to_first is None


def test_a_negative_elapsed_time_does_not_drag_the_median_down() -> None:
    """Floored at zero, so the median stays a wait somebody could have had."""
    users = [_new("a@x.edu"), _new("b@x.edu")]
    first_server = {
        "a@x.edu": _epoch("2026-07-04T08:00:00Z"),
        "b@x.edu": _epoch("2026-07-04T12:00:00Z"),
    }
    summary = summarize_adoption(users, first_server, timezone.utc)
    assert summary.median_seconds_to_first == (0 + 7200) / 2


@pytest.mark.parametrize("user", [{"name": "a@x.edu"}, {"name": "a@x.edu", "created": "then"}])
def test_a_user_with_no_usable_created_time_is_counted_but_not_timed(
    user: dict[str, Any],
) -> None:
    """They started a server; nothing says how long it took them to."""
    summary = summarize_adoption([user], {"a@x.edu": _epoch("2026-07-04T10:00:00Z")}, CHICAGO)
    assert (summary.new_users, summary.started) == (1, 1)
    assert summary.same_day == 0
    assert summary.median_seconds_to_first is None


def test_a_user_record_without_a_name_is_still_a_new_user() -> None:
    """The aggregation is safe on records the report itself would show."""
    assert summarize_adoption([{}], {}, timezone.utc) == AdoptionSummary(
        new_users=1, never_started=1
    )


def _new(name: str, created: str = "2026-07-04T10:00:00Z") -> dict[str, Any]:
    """Build the user record shape ``filter_new_users`` returns."""
    return {"name": name, "created": created}


def _epoch(text: str) -> int:
    """Return an ISO 8601 instant as the epoch seconds a mapping holds."""
    return int(datetime.fromisoformat(text).timestamp())


def _doc(
    *,
    user: str = "a@x.edu",
    started: str | None = "2026-07-04T10:00:00Z",
    snapshot: int = 1782000000,
) -> dict[str, Any]:
    """Build a flattened server snapshot for the first-server lookup."""
    doc: dict[str, Any] = {"user.name": user, "meta.snapshot-time": snapshot}
    if started is not None:
        doc["server.started"] = started
    return doc
