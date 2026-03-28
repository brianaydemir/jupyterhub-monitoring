"""Command-line tool for reporting active server time per JupyterHub user."""

import argparse
import sys
from typing import Any

from app.cli.runtime import parse_duration_required, run_command
from app.cli.utils import (
    configure_report_parser,
    make_es_client,
    validate_report_arguments,
)
from app.core.errors import ExternalServiceError
from app.core.time_utils import compute_time_range, parse_duration, parse_timezone
from app.reports.builders import build_activity_report
from app.reports.delivery import deliver_report


def build_query(
    cutoff: int,
    end: int,
    hub: str | None = None,
) -> dict[str, Any]:
    """Build the Elasticsearch Query DSL for active server documents."""
    filters: list[dict[str, Any]] = [
        {"range": {"meta.snapshot-time": {"gte": cutoff, "lte": end}}},
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
        filters.append({"term": {"meta.hub.keyword": hub}})

    return {
        "bool": {
            "filter": filters,
            "must_not": [{"term": {"meta.testing": "true"}}],
        }
    }


def compute_activity(documents: list[dict[str, Any]]) -> dict[str, float]:
    """Sum active server time per user from a list of Elasticsearch documents.

    Documents missing meta.interval are silently skipped.
    """
    totals: dict[str, float] = {}

    for doc in documents:
        user = doc.get("user.name")
        interval_str = doc.get("meta.interval")

        if not user or not interval_str:
            continue

        interval_td = parse_duration(interval_str)
        if interval_td is None:
            continue

        totals[user] = totals.get(user, 0.0) + interval_td.total_seconds()

    return totals


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser for this command."""
    parser = argparse.ArgumentParser(
        description="Report active server time per JupyterHub user from Elasticsearch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --es-endpoint https://es.example.com:9200 --es-api-key /path/to/api-key --es-index servers --duration "7 days"
  %(prog)s --es-endpoint https://es.example.com:9200 --es-api-key /path/to/api-key --es-index servers --duration "24h" --hub myhub
  %(prog)s --es-endpoint https://es.example.com:9200 --es-api-key /path/to/api-key --es-index servers --duration "7 days" --html-file report.html

Environment variables:
  ELASTICSEARCH_API_KEY  Elasticsearch API key (used when --es-api-key is not provided)
        """,
    )

    query_group = configure_report_parser(
        parser,
        source="es",
        default_subject="JupyterHub Activity Report",
    )
    query_group.add_argument(
        "--hub",
        help="Filter results to documents with this meta.hub value",
    )
    return parser


def _validate_arguments(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Run all post-parse argument validation for this command."""
    validate_report_arguments(args, parser, source="es")


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Raises:
        app.errors.ExternalServiceError: If Elasticsearch querying fails.
    """
    duration_td = parse_duration_required(args.duration)
    tz = parse_timezone(args.timezone)
    start_time, end_time = compute_time_range(duration_td, args.time, tz)
    cutoff = int(start_time.timestamp())

    try:
        with make_es_client(args) as client:
            query = build_query(cutoff=cutoff, end=int(end_time.timestamp()), hub=args.hub)
            documents = list(client.query(index=args.es_index, query=query))
    except Exception as e:
        raise ExternalServiceError(f"Querying Elasticsearch failed: {e}") from e

    totals = compute_activity(documents)

    report = build_activity_report(
        totals=totals,
        start_time=start_time,
        end_time=end_time,
        tz_name=args.timezone,
        detailed_usernames=args.detailed_usernames,
    )
    return deliver_report(args, report)


def main() -> int:
    """Main entry point for the get-new-activity script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
