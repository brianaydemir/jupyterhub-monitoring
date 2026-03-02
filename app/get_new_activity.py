"""Command-line tool for reporting active server time per JupyterHub user."""

import argparse
import sys
from datetime import timedelta

import pytimeparse2

from app.cli_utils import (
    add_elasticsearch_argument_group,
    add_email_argument_group,
    add_output_argument_group,
    add_query_argument_group,
    parse_duration,
    read_api_key,
    validate_elasticsearch_arguments,
    validate_email_arguments,
    validate_query_arguments,
)
from app.elasticsearch_client import ElasticsearchClient
from app.output_formatters import (
    format_activity_csv,
    format_activity_html,
    format_activity_text,
)
from app.send_email import send_report_email
from app.time_utils import compute_time_range, parse_timezone


def build_query(
    cutoff: int,
    end: int,
    hub: str | None = None,
) -> dict:
    """Build the Elasticsearch Query DSL for active server documents.

    Args:
        cutoff: Unix timestamp; only documents at or after this time are included
        end: Unix timestamp; only documents at or before this time are included
        hub: Optional meta.hub value to filter on

    Returns:
        Elasticsearch Query DSL dictionary
    """
    filters: list[dict] = [
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


def compute_activity(documents: list[dict]) -> dict[str, float]:
    """Sum active server time per user from a list of Elasticsearch documents.

    Documents missing meta.interval are silently skipped.

    Args:
        documents: List of Elasticsearch document source dictionaries

    Returns:
        Dictionary mapping user name to total active seconds
    """
    totals: dict[str, float] = {}

    for doc in documents:
        user = doc.get("user.name")
        interval_str = doc.get("meta.interval")

        if not user or not interval_str:
            continue

        seconds = pytimeparse2.parse(interval_str)
        if seconds is None:
            continue

        interval_seconds = (
            seconds.total_seconds() if isinstance(seconds, timedelta) else seconds
        )
        totals[user] = totals.get(user, 0.0) + interval_seconds

    return totals


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Report active server time per JupyterHub user from Elasticsearch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --elasticsearch-endpoint https://es.example.com:9200 --elasticsearch-api-key /path/to/api-key --elasticsearch-index servers --duration "7 days"
  %(prog)s --elasticsearch-endpoint https://es.example.com:9200 --elasticsearch-api-key /path/to/api-key --elasticsearch-index servers --duration "24h" --hub myhub
  %(prog)s --elasticsearch-endpoint https://es.example.com:9200 --elasticsearch-api-key /path/to/api-key --elasticsearch-index servers --duration "7 days" --html-file report.html

Environment variables:
  ELASTICSEARCH_API_KEY  Elasticsearch API key (used when --elasticsearch-api-key is not provided)
        """,
    )

    # Elasticsearch connection parameters
    add_elasticsearch_argument_group(parser)

    # Query parameters
    query_group = add_query_argument_group(parser)
    query_group.add_argument(
        "--hub",
        help="Filter results to documents with this meta.hub value",
    )

    # Output options
    add_output_argument_group(parser)

    # Email options
    add_email_argument_group(parser, default_subject="JupyterHub Activity Report")

    args = parser.parse_args()

    # Validate Elasticsearch API key and CA certificate
    validate_elasticsearch_arguments(args, parser)

    # Validate --time and --timezone
    validate_query_arguments(args, parser)

    # Validate email arguments
    validate_email_arguments(args, parser)

    return args


def main() -> int:
    """Main entry point for the get-new-activity script.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        args = parse_arguments()

        # Parse the duration string
        duration_td = parse_duration(args.duration)
        if duration_td is None:
            print(f"Error: Invalid duration format: {args.duration}", file=sys.stderr)
            return 1

        # Resolve timezone and compute time range
        tz = parse_timezone(args.timezone)
        start_time, end_time = compute_time_range(duration_td, args.time, tz)
        cutoff = int(start_time.timestamp())

        # Initialize the Elasticsearch client
        client = ElasticsearchClient(
            endpoint=args.elasticsearch_endpoint,
            api_key=read_api_key(args.elasticsearch_api_key, "ELASTICSEARCH_API_KEY"),
            ca_cert=(
                str(args.elasticsearch_ca_cert) if args.elasticsearch_ca_cert else None
            ),
        )

        # Query Elasticsearch
        try:
            query = build_query(
                cutoff=cutoff, end=int(end_time.timestamp()), hub=args.hub
            )
            documents = list(client.query(index=args.elasticsearch_index, query=query))
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"Error querying Elasticsearch: {e}", file=sys.stderr)
            return 1
        finally:
            client.close()

        # Aggregate active time per user
        totals = compute_activity(documents)

        # Always print the text report to stdout, bracketed by separator lines
        # so it remains visually distinct when stderr is merged into stdout
        text_content = format_activity_text(
            totals, start_time, end_time, args.timezone, args.detailed_usernames
        )
        print(f"---\n{text_content}\n---")

        # Output to text file if specified
        if args.text_file:
            args.text_file.write_text(text_content + "\n", encoding="utf-8")
            print(f"Plain text output written to: {args.text_file}", file=sys.stderr)

        # Output to HTML file if specified
        if args.html_file:
            html_content = format_activity_html(
                totals, start_time, end_time, args.timezone, args.detailed_usernames
            )
            args.html_file.write_text(html_content + "\n", encoding="utf-8")
            print(f"HTML output written to: {args.html_file}", file=sys.stderr)

        # Output to CSV file if specified
        if args.csv_file:
            csv_content = format_activity_csv(
                totals, start_time, end_time, args.timezone, args.detailed_usernames
            )
            args.csv_file.write_text(csv_content, encoding="utf-8")
            print(f"CSV output written to: {args.csv_file}", file=sys.stderr)

        # Send email if requested
        if args.send_email:
            html_content = format_activity_html(
                totals, start_time, end_time, args.timezone, args.detailed_usernames
            )
            csv_content = format_activity_csv(
                totals, start_time, end_time, args.timezone, args.detailed_usernames
            )
            if send_report_email(
                args, text_content, html_content, csv_content, "activity.csv"
            ):
                return 1

        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
