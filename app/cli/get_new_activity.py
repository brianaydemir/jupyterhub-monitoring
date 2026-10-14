"""Command-line tool for reporting active server time per JupyterHub user."""

import argparse
import sys
from typing import Any, Final

from app.cli.arguments import add_hub_argument
from app.cli.factories import (
    compute_report_time_range,
    make_es_client,
    make_output_targets,
    make_prometheus_client,
)
from app.cli.report_arguments import (
    ELASTICSEARCH_SOURCE,
    PROMETHEUS_SOURCE,
    configure_report_parser,
    validate_report_arguments,
)
from app.cli.runtime import run_command
from app.core.time_utils import DATETIME_FMT, parse_timezone
from app.reports.builders import build_activity_report
from app.reports.delivery import deliver_report
from app.usage.activity import (
    DocumentTally,
    SessionKey,
    SkipReason,
    compute_activity,
    summarize_activity,
    tally_documents,
)
from app.usage.memory import MemoryTally, measure_peak_memory
from app.usage.snapshots import build_query, select_hub

# The data this report draws on.
# Shared by the parser and the validator
# so that the two cannot disagree about which arguments exist.
_SOURCES: Final = (ELASTICSEARCH_SOURCE, PROMETHEUS_SOURCE)

# The fixed part of the default email subject,
# which the run completes with the reporting window.
# Shared with the parser, whose help for ``--subject`` names it.
_SUBJECT_PREFIX: Final = "JupyterHub Activity Report"


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for this command.

    There is no ``--date-format``: the ``Started`` column identifies a session,
    so a bare date would render a user's separate same-day runs of one server
    as rows differing only in a duration.
    """
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
  %(prog)s --es-endpoint https://es.example.com:9200 \\
    --es-api-key /path/to/key --es-index servers \\
    --duration "7 days" \\
    --prometheus-endpoint https://prometheus.example.com

Prometheus:
  Optional. Pass --prometheus-endpoint to also report each
  started session's peak memory. The snapshots must name
  each session's Pod, which push-servers records when its
  JupyterHub token has the admin:server_state scope.

Environment variables:
  ELASTICSEARCH_API_KEY  API key (when --es-api-key is
                         not provided)
  PROMETHEUS_TOKEN       Bearer token (when
                         --prometheus-token is not provided)
        """,
    )

    query_group = configure_report_parser(
        parser,
        sources=_SOURCES,
        subject_prefix=_SUBJECT_PREFIX,
        include_date_format=False,
    )
    add_hub_argument(query_group)
    return parser


def _validate_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Run post-parse argument validation."""
    validate_report_arguments(args, parser, sources=_SOURCES)


def _format_document_tally(tally: DocumentTally) -> str:
    """Render the retrieved/counted/skipped diagnostic for stderr.

    Skips are broken out by reason, which is what tells a report emptied
    by unreadable documents apart from one whose query found nothing.
    """
    noun = "document" if tally.retrieved == 1 else "documents"
    retrieved = f"Retrieved {tally.retrieved} {noun}"
    if not tally.retrieved:
        return f"{retrieved}."
    if not tally.skipped:
        return f"{retrieved}; counted {tally.counted}."
    reasons = ", ".join(
        f"{reason.value}: {tally.skipped[reason]}"
        for reason in SkipReason
        if reason in tally.skipped
    )
    skipped = sum(tally.skipped.values())
    return f"{retrieved}; counted {tally.counted}, skipped {skipped} ({reasons})."


def _format_memory_tally(tally: MemoryTally) -> str:
    """Render the measured/unmeasured session diagnostic for stderr.

    The two gaps are named apart because each has its own fix:
    sessions naming no Pod point at the ``push-servers`` token's scopes,
    and sessions with no samples at Prometheus's retention or labels.
    """
    noun = "session" if tally.started == 1 else "sessions"
    if not tally.started:
        return f"Measured peak memory for 0 started {noun}."
    measured = f"Measured peak memory for {tally.measured} of {tally.started} started {noun}"
    gaps: list[str] = []
    if tally.unnamed:
        gaps.append(f"{tally.unnamed} named no Pod")
    if tally.no_samples:
        gaps.append(f"{tally.no_samples} had no samples")
    if not gaps:
        return f"{measured}."
    return f"{measured}; {', '.join(gaps)}."


def _query_max_memory(
    args: argparse.Namespace,
    documents: list[dict[str, Any]],
) -> dict[SessionKey, float] | None:
    """Query Prometheus for each started session's peak memory.

    Returns:
        Each measured session's peak memory, in bytes,
        or ``None`` when ``--prometheus-endpoint`` was not given,
        which leaves the report's memory column out
        rather than filling it with ``"n/a"``.

    Raises:
        ExternalServiceError: If Prometheus querying fails.
    """
    if args.prometheus_endpoint is None:
        return None
    with make_prometheus_client(args) as client:
        peaks, tally = measure_peak_memory(documents, client)
    print(_format_memory_tally(tally), file=sys.stderr)
    return peaks


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    The document tally leads the report, and also goes to stderr,
    where a scheduled run's log keeps it apart from the report on stdout.
    So does the memory tally, when Prometheus is consulted.

    Raises:
        ExternalServiceError: If Elasticsearch or Prometheus querying fails.
    """
    tz = parse_timezone(args.timezone)
    start_time, end_time = compute_report_time_range(args)

    with make_es_client(args) as client:
        query = build_query(
            cutoff=int(start_time.timestamp()),
            end=int(end_time.timestamp()),
            hub=args.hub,
        )
        documents = list(select_hub(client.query(index=args.es_index, query=query), args.hub))

    tally = tally_documents(documents)
    print(_format_document_tally(tally), file=sys.stderr)

    max_memory = _query_max_memory(args, documents)

    sessions = compute_activity(documents)
    summary = summarize_activity(sessions, documents, tz, start_time, end_time)
    report = build_activity_report(
        sessions=sessions,
        summary=summary,
        tally=tally,
        start_time=start_time,
        end_time=end_time,
        strftime_fmt=DATETIME_FMT,
        detailed_usernames=args.detailed_usernames,
        anonymize=args.anonymize,
        max_memory=max_memory,
    )
    targets = make_output_targets(
        args,
        subject_prefix=_SUBJECT_PREFIX,
        window=(start_time, end_time),
    )
    return deliver_report(targets, report)


def main() -> int:
    """Main entry point for the get-new-activity script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
