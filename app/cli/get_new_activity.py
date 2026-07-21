"""Command-line tool for reporting active server time per JupyterHub user."""

import argparse
import re
import statistics
import sys
from collections import Counter
from collections.abc import Iterable
from datetime import datetime, tzinfo
from typing import Any

from app.cli.runtime import run_command
from app.cli.utils import (
    compute_report_time_range,
    configure_report_parser,
    get_strftime_fmt,
    make_es_client,
    validate_report_arguments,
)
from app.core.errors import AppError, ExternalServiceError
from app.core.time_utils import parse_duration
from app.reports.builders import (
    ActivitySummary,
    SessionActivity,
    build_activity_report,
)
from app.reports.delivery import deliver_report

# Matches the word "gpu" (case-insensitive) delimited by the start or end
# of the string or by any non-alphabetic character.
_GPU_RE = re.compile(r"(?<![a-zA-Z])gpu(?![a-zA-Z])", re.IGNORECASE)


def build_query(
    cutoff: int,
    end: int,
    hub: str | None = None,
) -> dict[str, Any]:
    """Build Elasticsearch Query DSL for active server documents."""
    filters: list[dict[str, Any]] = [
        {
            "range": {
                "meta.snapshot-time": {
                    "gte": cutoff,
                    "lte": end,
                }
            }
        },
        {
            "bool": {
                "should": [
                    {"term": {"server.ready": True}},
                    {"term": {"server.pending": "spawn"}},
                ],
                "minimum_should_match": 1,
            }
        },
    ]

    if hub is not None:
        filters.append(
            {
                "bool": {
                    "should": [
                        {"term": {"meta.hub.keyword": hub}},
                        {"term": {"meta.hub": hub}},
                    ],
                    "minimum_should_match": 1,
                }
            }
        )

    return {
        "bool": {
            "filter": filters,
            "must_not": [
                {
                    "bool": {
                        "should": [
                            {"term": {"meta.testing": "true"}},
                            {"term": {"meta.testing": True}},
                        ],
                        "minimum_should_match": 1,
                    }
                }
            ],
        }
    }


def _document_uses_gpu(doc: dict[str, Any]) -> bool:
    """Return whether a server document requested a GPU.

    Classifies as GPU when any ``server.user_options`` key or value
    contains the whole word "gpu" (case-insensitive).  The value is
    matched via its string form; for a list value this is equivalent to
    checking each element, since ``repr`` delimits elements with
    non-alphabetic characters that the word boundary already treats as
    breaks.
    """
    for key, value in doc.items():
        if key != "server.user_options" and not key.startswith("server.user_options."):
            continue
        if _GPU_RE.search(key) or _GPU_RE.search(str(value)):
            return True
    return False


def compute_activity(
    documents: Iterable[dict[str, Any]],
) -> list[SessionActivity]:
    """Sum active server time per session from Elasticsearch documents.

    A session is keyed by ``user.name`` together with ``server.started``,
    so concurrent named servers and separate runs are tallied
    independently.  A session counts as GPU when any of its snapshots
    requested a GPU.  Documents missing ``user.name`` or ``meta.interval``
    are silently skipped.
    """
    seconds: dict[tuple[str, str | None], float] = {}
    gpu: dict[tuple[str, str | None], bool] = {}

    for doc in documents:
        user = doc.get("user.name")
        interval_str = doc.get("meta.interval")

        if not isinstance(user, str) or not user:
            continue
        if not isinstance(interval_str, str) or not interval_str:
            continue

        interval_td = parse_duration(interval_str)
        if interval_td is None:
            continue

        started = doc.get("server.started")
        key = (user, started if isinstance(started, str) else None)

        seconds[key] = seconds.get(key, 0.0) + interval_td.total_seconds()
        gpu[key] = gpu.get(key, False) or _document_uses_gpu(doc)

    return [
        SessionActivity(user=user, started=started, seconds=total, gpu=gpu[(user, started)])
        for (user, started), total in seconds.items()
    ]


def _concurrency_stats(documents: list[dict[str, Any]]) -> tuple[int, float]:
    """Return peak and mean concurrent server counts across snapshots.

    Concurrency is the number of active-server documents sharing a
    ``meta.snapshot-time``.  The mean is taken over snapshots that had at
    least one active server; snapshots with none are not recorded.
    """
    counts = Counter(
        doc["meta.snapshot-time"]
        for doc in documents
        if isinstance(doc.get("meta.snapshot-time"), int)
    )
    if not counts:
        return 0, 0.0
    return max(counts.values()), statistics.mean(counts.values())


def _per_day_seconds(documents: list[dict[str, Any]], tz: tzinfo) -> dict[str, float]:
    """Bucket active server-time by calendar day in the report timezone."""
    per_day: dict[str, float] = {}
    for doc in documents:
        snapshot = doc.get("meta.snapshot-time")
        interval_str = doc.get("meta.interval")
        if not isinstance(snapshot, int):
            continue
        if not isinstance(interval_str, str) or not interval_str:
            continue
        interval_td = parse_duration(interval_str)
        if interval_td is None:
            continue
        day = datetime.fromtimestamp(snapshot, tz).strftime("%Y-%m-%d")
        per_day[day] = per_day.get(day, 0.0) + interval_td.total_seconds()
    return per_day


def summarize_activity(
    sessions: list[SessionActivity],
    documents: list[dict[str, Any]],
    tz: tzinfo,
) -> ActivitySummary:
    """Compute aggregate usage statistics for the activity report.

    Per-user, GPU, and session statistics are derived from *sessions*;
    concurrency and per-day activity require the raw snapshot *documents*.
    """
    user_seconds: dict[str, float] = {}
    gpu_users: set[str] = set()
    for session in sessions:
        user_seconds[session.user] = user_seconds.get(session.user, 0.0) + session.seconds
        if session.gpu:
            gpu_users.add(session.user)

    total_seconds = sum(session.seconds for session in sessions)
    gpu_seconds = sum(session.seconds for session in sessions if session.gpu)
    total_sessions = len(sessions)
    active_users = len(user_seconds)
    per_user = list(user_seconds.values())

    peak_concurrency, mean_concurrency = _concurrency_stats(documents)

    return ActivitySummary(
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
        mean_concurrency=mean_concurrency,
        per_day_seconds=_per_day_seconds(documents, tz),
    )


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for this command."""
    parser = argparse.ArgumentParser(
        description="Report active server time per JupyterHub user from Elasticsearch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --es-endpoint https://es.example.com:9200 \\
    --es-api-key /path/to/key --es-index servers \\
    --duration "7 days"
  %(prog)s --es-endpoint https://es.example.com:9200 \\
    --es-api-key /path/to/key --es-index servers \\
    --duration "24h" --hub myhub
  %(prog)s --es-endpoint https://es.example.com:9200 \\
    --es-api-key /path/to/key --es-index servers \\
    --report-start "July 1 2026" --report-end "July 8 2026"

Environment variables:
  ELASTICSEARCH_API_KEY  API key (when --es-api-key is
                         not provided)
        """,
    )

    query_group = configure_report_parser(
        parser,
        source="es",
        default_subject="JupyterHub Activity Report",
        include_date_format=True,
        date_format_default="datetime",
        include_anonymize=True,
    )
    query_group.add_argument(
        "--hub",
        help="Filter results to documents with this meta.hub value",
    )
    return parser


def _validate_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Run post-parse argument validation."""
    validate_report_arguments(args, parser, source="es")


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Raises:
        ExternalServiceError: If Elasticsearch querying
            fails.
    """
    start_time, end_time = compute_report_time_range(args)
    cutoff = int(start_time.timestamp())
    tz = start_time.tzinfo
    if tz is None:
        raise ValueError("start_time must be timezone-aware")

    try:
        with make_es_client(args) as client:
            query = build_query(
                cutoff=cutoff,
                end=int(end_time.timestamp()),
                hub=args.hub,
            )
            documents = list(client.query(index=args.es_index, query=query))
    except AppError:
        raise
    except Exception as e:
        raise ExternalServiceError(f"Querying Elasticsearch failed: {e}") from e

    sessions = compute_activity(documents)
    summary = summarize_activity(sessions, documents, tz)
    report = build_activity_report(
        sessions=sessions,
        summary=summary,
        start_time=start_time,
        end_time=end_time,
        tz_name=args.timezone,
        strftime_fmt=get_strftime_fmt(args),
        detailed_usernames=args.detailed_usernames,
        anonymize=args.anonymize,
    )
    return deliver_report(args, report)


def main() -> int:
    """Main entry point for the get-new-activity script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
