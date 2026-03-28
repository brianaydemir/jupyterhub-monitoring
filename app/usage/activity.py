"""Active server time, computed from server-snapshot documents.

A session is a user, one of that user's named servers, and that
server's start time. Activity is accumulated by summing each snapshot's
``meta.interval``, so every figure in a report is only as fine-grained
as the push cadence that produced the documents.

:class:`SessionActivity`, :class:`UserActivity`, and
:class:`ActivitySummary` are defined here, beside the code that produces
them; the report builders only consume them.
:class:`DocumentTally` is diagnostic rather than reported: it says how
many documents a run saw and why any of them could not be counted, which
is what tells an idle hub apart from a pipeline that fed the report
nothing it could read.
"""

import re
import statistics
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, tzinfo
from enum import Enum
from typing import Any, cast

from app.core.dataclass_defaults import empty_string_set
from app.core.time_utils import last_instant, parse_duration
from app.usage.snapshots import snapshot_seconds


@dataclass(frozen=True)
class SessionActivity:
    """Active server time for a single user session.

    A session is identified by a user, one of their named servers, and
    that server's ``server.started`` timestamp. *server_name* is the
    JupyterHub server name, empty for the default server; *started* is
    the raw ISO 8601 string, or ``None`` on snapshots taken while the
    server was still spawning.
    """

    user: str
    server_name: str | None
    started: str | None
    seconds: float
    gpu: bool


@dataclass(frozen=True)
class UserActivity:
    """Active server time for a single user, summed over their sessions.

    *sessions* counts every session the user holds, the spawn-only ones
    that carry no ``started`` included, so it agrees with the number of
    rows the per-session view shows for that user.
    """

    user: str
    sessions: int
    seconds: float
    gpu_seconds: float


# A local counterpart to the factories in
# :mod:`app.core.dataclass_defaults`,
# whose generic factories do not cover this field's type;
# see it for why these wrappers exist.
def _day_seconds() -> dict[str, float]:
    """Return an empty typed day-to-seconds mapping for dataclass defaults."""
    return {}


@dataclass(frozen=True)
class ActivitySummary:  # pylint: disable=too-many-instance-attributes
    """Aggregate usage statistics for a JupyterHub activity report.

    All durations are raw seconds; formatting is applied by the report
    builder. *per_day_seconds* maps ``"YYYY-MM-DD"`` to the active
    server-time observed on that day, and holds every calendar day of
    the reporting window, so a day that saw no activity carries zero
    rather than dropping out of an otherwise contiguous series.

    *window_seconds* is the reporting window's own length, which the
    report states so that its totals have a denominator to be read
    against. A window of no length, or an inverted one, is carried as
    zero, which is what *mean_concurrency_window* makes of it too, so
    the two figures derived from the window cannot disagree.

    The two concurrency means answer different questions.
    *mean_concurrency_active* is averaged over the pushes that had at
    least one active server, so it describes the hub while it is in use;
    *mean_concurrency_window* spreads the total active time over the
    whole reporting window, idle stretches included, which is what makes
    it comparable between windows.

    *hubs* holds the distinct ``meta.hub`` values the summarized
    documents carried, which is what lets a report say that its figures
    combine several hubs; the peak and *mean_concurrency_active* figures
    are per hub push, so they alone do not.
    """

    window_seconds: float = 0.0
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
    mean_concurrency_active: float = 0.0
    mean_concurrency_window: float = 0.0
    per_day_seconds: dict[str, float] = field(default_factory=_day_seconds)
    hubs: set[str] = field(default_factory=empty_string_set)


class SkipReason(Enum):
    """Why a snapshot could not be counted.

    Each value is the phrase a caller names the reason by, and the
    members are declared in the order :func:`_classify_document` checks
    them, so a caller can list the reasons it saw without spelling
    either the wording or the order itself.
    """

    NO_USER = "no user.name"
    NO_INTERVAL = "no meta.interval"
    BAD_INTERVAL = "unparsable meta.interval"
    NON_POSITIVE_INTERVAL = "non-positive meta.interval"


# A second local counterpart to the factories in
# :mod:`app.core.dataclass_defaults`; see :func:`_day_seconds`.
def _skip_counts() -> dict[SkipReason, int]:
    """Return an empty typed reason-to-count mapping for dataclass defaults."""
    return {}


@dataclass(frozen=True)
class DocumentTally:
    """How many snapshots a run retrieved, counted, and skipped.

    *skipped* holds only the reasons that actually occurred, and
    *counted* plus its values always sum to *retrieved*. Nothing in a
    report is derived from any of this: it exists so that a command can
    say what shape of data it saw, since a zero report reads the same
    whether the hub was idle, the query named the wrong index or hub, or
    every document retrieved was unreadable.
    """

    retrieved: int = 0
    counted: int = 0
    skipped: dict[SkipReason, int] = field(default_factory=_skip_counts)


# Matches the word "gpu" or "gpus" (case-insensitive) delimited by the
# start or end of the string or by any non-alphabetic character.
_GPU_RE = re.compile(r"(?<![a-zA-Z])gpus?(?![a-zA-Z])", re.IGNORECASE)

# Spellings a spawner form uses for an option that is switched off.
_DISABLED_OPTION_VALUES = frozenset({"", "0", "false", "no", "none", "off"})

# The format ``per_day_seconds`` is keyed by. The bucketing and the
# zero-fill share it, so the two cannot disagree about a day's key.
_DAY_FMT = "%Y-%m-%d"

# A session: a user, one of their named servers, and that server's start.
_SessionKey = tuple[str, str | None, str | None]

# One hub's one push: the hub that wrote the documents, or ``None`` when
# they name none, and the snapshot time it stamped every one of them with.
_PushKey = tuple[str | None, int]


def _option_is_enabled(value: Any) -> bool:
    """Return whether a ``user_options`` value reads as switched on."""
    if isinstance(value, str):
        return value.strip().lower() not in _DISABLED_OPTION_VALUES
    return bool(value)


def _value_names_gpu(value: Any) -> bool:
    """Return whether a ``user_options`` value names a GPU resource.

    A string counts when it contains the word anywhere, per ``_GPU_RE``,
    so ``cpu_and_gpu`` counts but ``nogpu`` does not. A list is checked
    element-wise, which is what lets a multi-select's ``["gpu", "large"]``
    count.
    """
    if isinstance(value, str):
        return _GPU_RE.search(value) is not None
    if isinstance(value, (list, tuple, set, frozenset)):
        return any(_value_names_gpu(item) for item in cast(Iterable[Any], value))
    return False


def _document_uses_gpu(doc: dict[str, Any]) -> bool:
    """Return whether a server document requested a GPU.

    A ``server.user_options`` entry counts when its value names the
    word, per :func:`_value_names_gpu`, or when its *key* names one and
    the value reads as switched on. Requiring a switched-on value for a
    key match is what keeps a spawner form's unchecked ``gpu: false``
    box from billing the session as GPU time.

    Both rules depend on the options arriving as flattened dotted keys,
    which is what ``push-servers`` writes: a value that is itself a
    mapping is not searched, so a GPU entry nested inside one is missed.
    """
    for key, value in doc.items():
        if key != "server.user_options" and not key.startswith("server.user_options."):
            continue
        if _value_names_gpu(value):
            return True
        if _GPU_RE.search(key) and _option_is_enabled(value):
            return True
    return False


def _classify_document(doc: dict[str, Any]) -> tuple[str, float] | SkipReason:
    """Return ``(user, seconds)`` for a countable snapshot, else why not.

    The rule lives here alone, so the tally :func:`tally_documents`
    reports and the contributions the metrics sum cannot disagree about
    which documents count.

    A zero or negative ``meta.interval`` parses, so the sign is checked
    here or nowhere. ``push-servers`` refuses to write one, but any
    other writer with access to the index may, and counting it would
    subtract that interval from every figure it reaches, or manufacture
    a session that inflates the session count and deflates the mean
    session length while contributing no time at all.
    """
    user = doc.get("user.name")
    if not isinstance(user, str) or not user:
        return SkipReason.NO_USER

    interval_str = doc.get("meta.interval")
    if not isinstance(interval_str, str) or not interval_str:
        return SkipReason.NO_INTERVAL

    interval_td = parse_duration(interval_str)
    if interval_td is None:
        return SkipReason.BAD_INTERVAL

    seconds = interval_td.total_seconds()
    if seconds <= 0:
        return SkipReason.NON_POSITIVE_INTERVAL

    return user, seconds


def _document_contribution(doc: dict[str, Any]) -> tuple[str, float] | None:
    """Return ``(user, seconds)`` for a countable snapshot, else ``None``.

    A snapshot counts only when it carries a non-empty ``user.name`` and
    a ``meta.interval`` that parses to a positive span. Every activity
    metric applies this same rule, so the per-session, per-day, and
    concurrency figures are all derived from one document set. This
    drops the reason :func:`_classify_document` gives, which no metric
    has a use for.
    """
    outcome = _classify_document(doc)
    return None if isinstance(outcome, SkipReason) else outcome


def _document_hub(doc: dict[str, Any]) -> str | None:
    """Return the hub a document names, or ``None`` when it names none.

    ``push-servers`` writes whatever ``--metadata hub=...`` gave it, and
    :func:`~app.usage.snapshots.build_query` matches the field as both
    text and keyword, so the value is usually a string but need not be.
    Any other scalar is rendered with :func:`str`, which keeps every hub
    hashable and printable; an absent field stays ``None`` and forms a
    group of its own.
    """
    value = doc.get("meta.hub")
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def compute_activity(
    documents: Iterable[dict[str, Any]],
) -> list[SessionActivity]:
    """Sum active server time per session from Elasticsearch documents.

    A session is keyed by ``user.name``, ``server.name``, and
    ``server.started``, so concurrent named servers and separate runs are
    tallied independently. Snapshots taken while a server is still
    spawning carry no ``server.started``, so a user's spawn attempts on
    one named server share a single session. A session counts as GPU
    when any of its snapshots requested one, and documents that
    :func:`_document_contribution` rejects contribute nothing;
    :func:`tally_documents` is what counts those.
    """
    seconds: dict[_SessionKey, float] = {}
    gpu: dict[_SessionKey, bool] = {}

    for doc in documents:
        contribution = _document_contribution(doc)
        if contribution is None:
            continue
        user, doc_seconds = contribution

        server = doc.get("server.name")
        started = doc.get("server.started")
        key = (
            user,
            server if isinstance(server, str) else None,
            started if isinstance(started, str) else None,
        )

        seconds[key] = seconds.get(key, 0.0) + doc_seconds
        gpu[key] = gpu.get(key, False) or _document_uses_gpu(doc)

    return [
        SessionActivity(
            user=user,
            server_name=server,
            started=started,
            seconds=total,
            gpu=gpu[(user, server, started)],
        )
        for (user, server, started), total in seconds.items()
    ]


def tally_documents(documents: Iterable[dict[str, Any]]) -> DocumentTally:
    """Count the documents retrieved, the countable ones, and the rest.

    Each metric makes its own pass over the documents and consults
    :func:`_document_contribution` on that pass, so a counter kept inside
    that gate would count one document once per metric. The tally is
    instead a pass of its own that classifies each document exactly once,
    and it holds no state for a later pass to touch.

    Counting nothing is not by itself a fault: an idle window
    legitimately yields no documents at all. It is the reasons that tell
    that window apart from a run whose documents were all discarded, so
    they are counted separately rather than summed.
    """
    retrieved = 0
    counted = 0
    skipped: dict[SkipReason, int] = {}

    for doc in documents:
        retrieved += 1
        outcome = _classify_document(doc)
        if isinstance(outcome, SkipReason):
            skipped[outcome] = skipped.get(outcome, 0) + 1
        else:
            counted += 1

    return DocumentTally(retrieved=retrieved, counted=counted, skipped=skipped)


def aggregate_user_activity(sessions: Iterable[SessionActivity]) -> list[UserActivity]:
    """Sum each user's sessions into a single row per user.

    Users come back in the order they first appear in *sessions*, since
    the order a report shows them in is the report's own decision. The
    summary's per-user statistics read this same function, so a report's
    per-user table and its ``Max time (one user)`` cannot disagree.
    """
    counts: dict[str, int] = {}
    seconds: dict[str, float] = {}
    gpu_seconds: dict[str, float] = {}

    for session in sessions:
        counts[session.user] = counts.get(session.user, 0) + 1
        seconds[session.user] = seconds.get(session.user, 0.0) + session.seconds
        if session.gpu:
            gpu_seconds[session.user] = gpu_seconds.get(session.user, 0.0) + session.seconds

    return [
        UserActivity(
            user=user,
            sessions=count,
            seconds=seconds[user],
            gpu_seconds=gpu_seconds.get(user, 0.0),
        )
        for user, count in counts.items()
    ]


def _concurrency_stats(documents: list[dict[str, Any]]) -> tuple[int, float]:
    """Return peak and mean concurrent server counts, per hub push.

    Concurrency is the number of countable documents sharing a
    ``(meta.hub, meta.snapshot-time)`` pair, which is one per active
    server because ``push-servers`` writes one document per server per
    push and stamps every one of them with that push's single snapshot
    time. Grouping on the pair rather than on the time alone is what
    makes the figures deterministic on a multi-hub index: two hubs whose
    pushes happen to land on the same second stay apart, instead of
    merging into one group whenever their schedules coincide.

    Both figures are therefore per hub push rather than cluster-wide:
    peak is the largest single push, and the mean is taken over the
    pushes that had at least one active server, since the query returns
    no documents for an idle one. That mean is the summary's
    *mean_concurrency_active*; the window-wide counterpart is
    :func:`_mean_concurrency_window`.
    """
    counts: Counter[_PushKey] = Counter()
    for doc in documents:
        snapshot = snapshot_seconds(doc)
        if snapshot is None or _document_contribution(doc) is None:
            continue
        counts[(_document_hub(doc), snapshot)] += 1
    if not counts:
        return 0, 0.0
    return max(counts.values()), statistics.mean(counts.values())


def _mean_concurrency_window(total_seconds: float, window: timedelta) -> float:
    """Return the active server time spread over the whole *window*.

    Unlike the per-push mean, this counts the window's idle stretches,
    which is what makes it comparable between reporting windows. A
    window of no length reports zero rather than dividing by it; the
    commands reject such a window, but a caller need not.
    """
    window_seconds = window.total_seconds()
    if window_seconds <= 0:
        return 0.0
    return total_seconds / window_seconds


def _observed_hubs(documents: list[dict[str, Any]]) -> set[str]:
    """Return the distinct hubs the countable documents name.

    Documents naming no hub contribute nothing, so a single-hub index
    that never sets ``meta.hub`` reports an empty set rather than an
    unnamed hub. Only countable documents are considered, matching the
    document set every other figure is derived from.
    """
    hubs: set[str] = set()
    for doc in documents:
        if _document_contribution(doc) is None:
            continue
        hub = _document_hub(doc)
        if hub is not None:
            hubs.add(hub)
    return hubs


def _window_days(start_time: datetime, end_time: datetime, tz: tzinfo) -> list[str]:
    """Return a day key for every calendar day in *tz* the window holds.

    The window is half-open, so its last day is the one holding its last
    instant, not the one holding *end_time*: a window ending exactly on
    local midnight stops short of that day, and a row for it would be a
    day the report could not have observed. A window of no length holds
    no instant at all, and an inverted one no day, so both yield nothing,
    which is what the other window-derived figures make of them too.

    Days are stepped through as calendar dates rather than by adding 24
    hours, so a DST transition neither skips nor repeats one.
    """
    if end_time <= start_time:
        return []
    day = start_time.astimezone(tz).date()
    last_day = last_instant(end_time).astimezone(tz).date()
    days: list[str] = []
    while day <= last_day:
        days.append(day.strftime(_DAY_FMT))
        day += timedelta(days=1)
    return days


def _per_day_seconds(
    documents: list[dict[str, Any]],
    tz: tzinfo,
    start_time: datetime,
    end_time: datetime,
) -> dict[str, float]:
    """Bucket active server-time by calendar day in the report timezone.

    Every day of the reporting window is present, per
    :func:`_window_days`, so the series a report renders is contiguous
    and a quiet day reads as zero instead of vanishing between the days
    around it.

    A snapshot's ``meta.interval`` covers the period *preceding* it, but
    is credited to the day the snapshot falls on, so activity in the last
    interval before local midnight is counted against the following day.
    A snapshot outside the window still gets a day of its own: the
    report's query returns no such document, but a direct caller may
    supply one, and dropping it would hide it.
    """
    per_day: dict[str, float] = dict.fromkeys(_window_days(start_time, end_time, tz), 0.0)
    for doc in documents:
        snapshot = snapshot_seconds(doc)
        if snapshot is None:
            continue
        contribution = _document_contribution(doc)
        if contribution is None:
            continue
        day = datetime.fromtimestamp(snapshot, tz).strftime(_DAY_FMT)
        per_day[day] = per_day.get(day, 0.0) + contribution[1]
    return per_day


def summarize_activity(
    sessions: list[SessionActivity],
    documents: list[dict[str, Any]],
    tz: tzinfo,
    start_time: datetime,
    end_time: datetime,
) -> ActivitySummary:
    """Compute aggregate usage statistics for the activity report.

    Per-user, GPU, and session statistics are derived from *sessions*,
    the per-user ones through :func:`aggregate_user_activity`, so the
    summary agrees with a report's own per-user table; the per-push
    concurrency figures, per-day activity, and the hubs observed require
    the raw snapshot *documents*.

    The reporting window arrives as its bounds rather than as its length,
    since the calendar days ``per_day_seconds`` covers cannot be
    recovered from a length alone. The length the summary carries,
    floored at zero, and the figures measured against the window as a
    whole are derived from that same pair, so no two of them can describe
    different windows. Both bounds are read in *tz*, so a naive one is
    read against the host's clock; the command rejects such a bound
    before it reaches here.
    """
    window = end_time - start_time
    users = aggregate_user_activity(sessions)
    gpu_users = {session.user for session in sessions if session.gpu}

    total_seconds = sum(session.seconds for session in sessions)
    gpu_seconds = sum(session.seconds for session in sessions if session.gpu)
    total_sessions = len(sessions)
    active_users = len(users)
    per_user = [user.seconds for user in users]

    peak_concurrency, mean_concurrency_active = _concurrency_stats(documents)

    return ActivitySummary(
        window_seconds=max(window.total_seconds(), 0.0),
        total_seconds=total_seconds,
        gpu_seconds=gpu_seconds,
        active_users=active_users,
        total_sessions=total_sessions,
        gpu_users=len(gpu_users),
        gpu_sessions=sum(1 for session in sessions if session.gpu),
        mean_user_seconds=statistics.mean(per_user) if per_user else 0.0,
        median_user_seconds=statistics.median(per_user) if per_user else 0.0,
        max_user_seconds=max(per_user) if per_user else 0.0,
        mean_sessions_per_user=total_sessions / active_users if active_users else 0.0,
        mean_session_seconds=total_seconds / total_sessions if total_sessions else 0.0,
        peak_concurrency=peak_concurrency,
        mean_concurrency_active=mean_concurrency_active,
        mean_concurrency_window=_mean_concurrency_window(total_seconds, window),
        per_day_seconds=_per_day_seconds(documents, tz, start_time, end_time),
        hubs=_observed_hubs(documents),
    )
