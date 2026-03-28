"""Selecting newly created users and dating their first server.

The user records come from JupyterHub; the first-server times come from
the same server-snapshot documents the activity report reads.
:class:`AdoptionSummary` combines the two, so that a report can say how
many of the new users went on to use the hub.
"""

import statistics
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, tzinfo
from typing import Any

from app.core.time_utils import parse_iso_utc
from app.usage.snapshots import snapshot_seconds, started_seconds


@dataclass(frozen=True)
class AdoptionSummary:
    """How many of a report's new users went on to start a server.

    Counts are per new-user record rather than per distinct username, so
    that *new_users* is the number of rows the report's table shows and
    *started* and *never_started* sum to it.

    *median_seconds_to_first* is raw seconds, or ``None`` when no elapsed
    time could be measured; formatting is the report builder's job.
    """

    new_users: int = 0
    started: int = 0
    never_started: int = 0
    same_day: int = 0
    median_seconds_to_first: float | None = None


def filter_new_users(
    users: Iterable[dict[str, Any]],
    cutoff_time: datetime,
    end_time: datetime,
) -> list[dict[str, Any]]:
    """Filter users created within the half-open window ``[start, end)``.

    The window excludes its own end, matching
    :func:`~app.usage.snapshots.build_query`, so that back-to-back
    reports do not both count a user created on their shared boundary.
    Users whose ``created`` value is missing or unparsable are skipped.
    """
    new_users: list[dict[str, Any]] = []
    for user in users:
        created_str = user.get("created")
        created_dt = parse_iso_utc(created_str)
        if created_dt is None:
            continue

        if cutoff_time <= created_dt < end_time:
            new_users.append(
                {
                    "name": user.get("name", ""),
                    "created": created_str,
                }
            )

    return new_users


def compute_first_server(
    documents: Iterable[dict[str, Any]],
) -> dict[str, int]:
    """Find each user's earliest server start, in epoch seconds.

    The time comes from the earliest ``server.started`` seen for that
    user. A snapshot taken while the server was still spawning carries
    no ``server.started``, so those fall back to ``meta.snapshot-time``,
    which is late by at most one push interval. Documents with neither,
    or without a usable ``user.name``, are silently skipped.
    """
    earliest: dict[str, int] = {}

    for doc in documents:
        user = doc.get("user.name")
        if not isinstance(user, str) or not user:
            continue

        ts = started_seconds(doc)
        if ts is None:
            ts = snapshot_seconds(doc)
        if ts is None:
            continue

        if user not in earliest or ts < earliest[user]:
            earliest[user] = ts

    return earliest


def summarize_adoption(
    users: Iterable[dict[str, Any]],
    first_server: dict[str, int],
    tz: tzinfo,
) -> AdoptionSummary:
    """Summarize how many new users went on to start a server.

    *users* holds the records :func:`filter_new_users` returns and
    *first_server* the mapping :func:`compute_first_server` returns; a
    user absent from the mapping never started a server. Same-day starts
    are judged in *tz*, the zone the report renders its timestamps in, so
    the count cannot contradict the table the reader is looking at.

    An elapsed time is floored at zero. :func:`compute_first_server`
    falls back to a snapshot time, and the search that feeds it opens at
    the start of the reporting window rather than at each user's own
    creation, so a first server can be dated just before the user it
    belongs to. Reading that as an immediate start keeps the median over
    exactly the users *started* counts, which discarding it would not.

    A record whose ``created`` value is unparsable still counts as a new
    user, and as having started a server when the mapping holds it, but
    contributes to neither the same-day count nor the median: nothing
    places it in time. The median is therefore ``None`` whenever no
    elapsed time could be measured, not only when nobody started.
    """
    total = 0
    started = 0
    same_day = 0
    elapsed: list[float] = []

    for user in users:
        total += 1

        ts = first_server.get(user.get("name", ""))
        if ts is None:
            continue
        started += 1

        created = parse_iso_utc(user.get("created"))
        if created is None:
            continue

        first = datetime.fromtimestamp(ts, tz)
        if created.astimezone(tz).date() == first.date():
            same_day += 1
        elapsed.append(max((first - created).total_seconds(), 0.0))

    return AdoptionSummary(
        new_users=total,
        started=started,
        never_started=total - started,
        same_day=same_day,
        median_seconds_to_first=statistics.median(elapsed) if elapsed else None,
    )
