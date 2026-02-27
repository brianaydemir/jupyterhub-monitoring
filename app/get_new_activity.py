"""Command-line tool for reporting active server time per JupyterHub user."""

import argparse
import html
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import humanize
import pytimeparse2

from app.elasticsearch_client import ElasticsearchClient


def build_query(
    cutoff: int,
    hub: str | None = None,
) -> dict:
    """Build the Elasticsearch Query DSL for active server documents.

    Args:
        cutoff: Unix timestamp; only documents at or after this time are included
        hub: Optional meta.hub value to filter on

    Returns:
        Elasticsearch Query DSL dictionary
    """
    filters: list[dict] = [
        {"range": {"meta.snapshot-time": {"gte": cutoff}}},
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
        filters.append({"term": {"meta.hub": hub}})

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


def _format_duration(seconds: float) -> str:
    """Format a duration in seconds as a human-readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        Human-readable duration string (e.g., "2 hours, 30 minutes")
    """
    return humanize.precisedelta(timedelta(seconds=seconds))


def _sorted_rows(totals: dict[str, float]) -> list[tuple[str, float]]:
    """Return (user, seconds) pairs sorted by time descending, then name ascending."""
    return sorted(totals.items(), key=lambda item: (-item[1], item[0]))


def format_output_text(totals: dict[str, float], duration_str: str) -> str:
    """Format per-user activity as plain text.

    Args:
        totals: Dictionary mapping user name to total active seconds
        duration_str: Human-readable duration string for the report header

    Returns:
        Plain text formatted string with a two-column table
    """
    lines = [f"Active server time in the last {duration_str}:", ""]

    if not totals:
        lines.append("No active server time found.")
    else:
        rows = [
            (user, _format_duration(seconds)) for user, seconds in _sorted_rows(totals)
        ]
        name_width = max(len("Name"), max(len(r[0]) for r in rows))
        time_width = max(len("Active time"), max(len(r[1]) for r in rows))
        lines.append(f"{'Name':<{name_width}}  {'Active time':<{time_width}}")
        lines.append(f"{'-' * name_width}  {'-' * time_width}")
        for name, active_time in rows:
            lines.append(f"{name:<{name_width}}  {active_time:<{time_width}}")

    return "\n".join(lines)


def format_output_html(totals: dict[str, float], duration_str: str) -> str:
    """Format per-user activity as HTML suitable for an email body.

    Args:
        totals: Dictionary mapping user name to total active seconds
        duration_str: Human-readable duration string for the report header

    Returns:
        HTML formatted string (body content only)
    """
    html_lines = [f"<p>Active server time in the last {html.escape(duration_str)}:</p>"]

    if not totals:
        html_lines.append("<p>No active server time found.</p>")
    else:
        html_lines.append("<table>")
        html_lines.append("  <thead>")
        html_lines.append(
            "    <tr>"
            '<th style="text-align:left">Name</th>'
            '<th style="text-align:left">Active time</th>'
            "</tr>"
        )
        html_lines.append("  </thead>")
        html_lines.append("  <tbody>")
        for user, seconds in _sorted_rows(totals):
            name = html.escape(user)
            active_time = html.escape(_format_duration(seconds))
            html_lines.append(f"    <tr><td>{name}</td><td>{active_time}</td></tr>")
        html_lines.append("  </tbody>")
        html_lines.append("</table>")

    return "\n".join(html_lines)


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
  %(prog)s --endpoint https://es.example.com:9200 --api-key /path/to/api-key --index servers --duration "7 days"
  %(prog)s --endpoint https://es.example.com:9200 --api-key /path/to/api-key --index servers --duration "24h" --hub myhub
  %(prog)s --endpoint https://es.example.com:9200 --api-key /path/to/api-key --index servers --duration "7 days" --html-file report.html

Environment variables:
  ELASTICSEARCH_API_KEY  Elasticsearch API key (used when --api-key is not provided)
        """,
    )

    # Elasticsearch connection parameters
    parser.add_argument(
        "--endpoint",
        required=True,
        help="Elasticsearch API endpoint URL (e.g., https://localhost:9200)",
    )
    parser.add_argument(
        "--api-key",
        type=Path,
        help=(
            "Path to file containing the Elasticsearch API key for authentication "
            "(or set ELASTICSEARCH_API_KEY)"
        ),
    )
    parser.add_argument(
        "--ca-cert",
        type=Path,
        help="Path to CA certificate file for TLS verification",
    )

    # Query parameters
    parser.add_argument(
        "--index",
        required=True,
        help="Name of the Elasticsearch index to query",
    )
    parser.add_argument(
        "--duration",
        required=True,
        help=(
            'Time window to look back from now (e.g., "30 seconds", "15 min", '
            '"12h", "7 days", "3d 6h 12m")'
        ),
    )
    parser.add_argument(
        "--hub",
        help="Filter results to documents with this meta.hub value",
    )

    # Output options
    parser.add_argument(
        "--text-file",
        type=Path,
        help="Write output as plain text to the specified file",
    )
    parser.add_argument(
        "--html-file",
        type=Path,
        help="Write output as HTML to the specified file (suitable for email body)",
    )

    args = parser.parse_args()

    # Validate CA certificate exists if provided
    if args.ca_cert and not args.ca_cert.exists():
        parser.error(f"CA certificate file not found: {args.ca_cert}")

    # Validate API key: file arg takes precedence over env var
    if args.api_key is not None:
        if not args.api_key.exists():
            parser.error(f"API key file not found: {args.api_key}")
    elif not os.environ.get("ELASTICSEARCH_API_KEY"):
        parser.error(
            "--api-key or the ELASTICSEARCH_API_KEY environment variable is required"
        )

    return args


def main() -> int:
    """Main entry point for the get-new-activity script.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        args = parse_arguments()

        # Parse the duration string
        duration_seconds = pytimeparse2.parse(args.duration)
        if duration_seconds is None:
            print(f"Error: Invalid duration format: {args.duration}", file=sys.stderr)
            return 1

        # Compute the cutoff Unix timestamp
        duration_td = (
            duration_seconds
            if isinstance(duration_seconds, timedelta)
            else timedelta(seconds=duration_seconds)
        )
        cutoff = int((datetime.now(timezone.utc) - duration_td).timestamp())

        # Initialize the Elasticsearch client
        api_key = (
            args.api_key.read_text().strip()
            if args.api_key
            else os.environ["ELASTICSEARCH_API_KEY"]
        )
        client = ElasticsearchClient(
            endpoint=args.endpoint,
            api_key=api_key,
            ca_cert=str(args.ca_cert) if args.ca_cert else None,
        )

        # Query Elasticsearch
        try:
            query = build_query(cutoff=cutoff, hub=args.hub)
            documents = list(client.query(index=args.index, query=query))
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"Error querying Elasticsearch: {e}", file=sys.stderr)
            return 1
        finally:
            client.close()

        # Aggregate active time per user
        totals = compute_activity(documents)

        # Build human-readable duration for report headers
        human_duration = humanize.naturaldelta(
            duration_seconds
            if isinstance(duration_seconds, timedelta)
            else timedelta(seconds=duration_seconds)
        )

        # Output to stdout by default
        if not args.text_file and not args.html_file:
            print(format_output_text(totals, human_duration))

        # Output to text file if specified
        if args.text_file:
            text_content = format_output_text(totals, human_duration)
            args.text_file.write_text(text_content + "\n", encoding="utf-8")
            print(f"Plain text output written to: {args.text_file}", file=sys.stderr)

        # Output to HTML file if specified
        if args.html_file:
            html_content = format_output_html(totals, human_duration)
            args.html_file.write_text(html_content + "\n", encoding="utf-8")
            print(f"HTML output written to: {args.html_file}", file=sys.stderr)

        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
