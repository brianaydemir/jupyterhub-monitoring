"""Command-line tool for reporting active server time per JupyterHub user."""

import argparse
import html
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import humanize
import pytimeparse2

from app.elasticsearch_client import ElasticsearchClient
from app.name_utils import _trailing_domain_key, parse_name
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


def _format_duration(seconds: float) -> str:
    """Format a duration in seconds as a human-readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        Human-readable duration string (e.g., "2 hours, 30 minutes")
    """
    return humanize.precisedelta(timedelta(seconds=seconds))


def _sorted_rows(
    totals: dict[str, float], show_method: bool
) -> list[tuple[str, float]]:
    """Return (user, seconds) pairs sorted by time descending, then by parsed name.

    When *show_method* is True, sorts by (priority, domain, id) as a tiebreaker.
    When False, sorts by (trailing_domain_key, domain, id) so trailing domains
    (e.g. orcid.org) sort last regardless of login method.
    """
    if show_method:
        name_key = lambda user: parse_name(user)[:3]  # noqa: E731
    else:
        name_key = lambda user: (  # noqa: E731
            _trailing_domain_key(parse_name(user)[1]),
            parse_name(user)[1],
            parse_name(user)[2],
        )
    return sorted(totals.items(), key=lambda item: (-item[1], name_key(item[0])))


def format_output_text(
    totals: dict[str, float],
    start_time: datetime,
    end_time: datetime,
    tz_name: str,
    detailed_usernames: bool = False,
) -> str:
    """Format per-user activity as plain text.

    Args:
        totals: Dictionary mapping user name to total active seconds
        start_time: Start of the reporting window (timezone-aware)
        end_time: End of the reporting window (timezone-aware)
        tz_name: Timezone name for the footnote
        detailed_usernames: Always show the "Login method" column

    Returns:
        Plain text formatted string with a table
    """
    fmt = "%Y-%m-%d %H:%M"
    range_str = f"from {start_time.strftime(fmt)} to {end_time.strftime(fmt)}"
    n = len(totals)
    if n == 0:
        lines = [f"No active server time between {start_time.strftime(fmt)} and {end_time.strftime(fmt)}."]
    else:
        noun = "user" if n == 1 else "users"
        lines = [f"{n} {noun} with active server time {range_str}:", ""]
        parsed_names = {user: parse_name(user) for user in totals}
        domain_id_pairs = [(p[1], p[2]) for p in parsed_names.values()]
        show_method = detailed_usernames or (
            len(domain_id_pairs) != len(set(domain_id_pairs))
        )
        rows = [
            (*parsed_names[user], _format_duration(seconds))
            for user, seconds in _sorted_rows(totals, show_method)
        ]
        domain_width = max(len("Domain"), max(len(r[1]) for r in rows))
        id_width = max(len("ID"), max(len(r[2]) for r in rows))
        time_width = max(len("Active time"), max(len(r[4]) for r in rows))
        if show_method:
            method_width = max(len("Login method"), max(len(r[3]) for r in rows))
            lines.append(
                f"{'Domain':<{domain_width}}  {'ID':<{id_width}}  "
                f"{'Login method':<{method_width}}  {'Active time':<{time_width}}"
            )
            lines.append(
                f"{'-' * domain_width}  {'-' * id_width}  "
                f"{'-' * method_width}  {'-' * time_width}"
            )
            for _priority, domain, uid, method, active_time in rows:
                lines.append(
                    f"{domain:<{domain_width}}  {uid:<{id_width}}  "
                    f"{method:<{method_width}}  {active_time:<{time_width}}"
                )
        else:
            lines.append(
                f"{'Domain':<{domain_width}}  {'ID':<{id_width}}  "
                f"{'Active time':<{time_width}}"
            )
            lines.append(
                f"{'-' * domain_width}  {'-' * id_width}  {'-' * time_width}"
            )
            for _priority, domain, uid, _method, active_time in rows:
                lines.append(
                    f"{domain:<{domain_width}}  {uid:<{id_width}}  "
                    f"{active_time:<{time_width}}"
                )

    lines.append("")
    lines.append(f"Timezone: {tz_name}")
    return "\n".join(lines)


def format_output_html(
    totals: dict[str, float],
    start_time: datetime,
    end_time: datetime,
    tz_name: str,
    detailed_usernames: bool = False,
) -> str:
    """Format per-user activity as HTML suitable for an email body.

    Args:
        totals: Dictionary mapping user name to total active seconds
        start_time: Start of the reporting window (timezone-aware)
        end_time: End of the reporting window (timezone-aware)
        tz_name: Timezone name for the footnote
        detailed_usernames: Always show the "Login method" column

    Returns:
        HTML formatted string (body content only)
    """
    fmt = "%Y-%m-%d %H:%M"
    range_str = f"from {html.escape(start_time.strftime(fmt))} to {html.escape(end_time.strftime(fmt))}"
    n = len(totals)

    if not totals:
        html_lines = [
            f"<p>No active server time between "
            f"{html.escape(start_time.strftime(fmt))} and "
            f"{html.escape(end_time.strftime(fmt))}.</p>"
        ]
    else:
        noun = "user" if n == 1 else "users"
        html_lines = [
            f"<p>{n} {noun} with active server time {range_str}:</p>"
        ]
        parsed_names = {user: parse_name(user) for user in totals}
        domain_id_pairs = [(p[1], p[2]) for p in parsed_names.values()]
        show_method = detailed_usernames or (
            len(domain_id_pairs) != len(set(domain_id_pairs))
        )
        TH = 'text-align:left; border:1px solid #9ab3c8; padding:2px 8px; background:#bdd7ee; color:#000000'
        html_lines.append('<table style="border-collapse:collapse">')
        html_lines.append("  <thead>")
        if show_method:
            html_lines.append(
                f'    <tr>'
                f'<th style="{TH}">Domain</th>'
                f'<th style="{TH}">ID</th>'
                f'<th style="{TH}">Login method</th>'
                f'<th style="{TH}">Active time</th>'
                f"</tr>"
            )
        else:
            html_lines.append(
                f'    <tr>'
                f'<th style="{TH}">Domain</th>'
                f'<th style="{TH}">ID</th>'
                f'<th style="{TH}">Active time</th>'
                f"</tr>"
            )
        html_lines.append("  </thead>")
        html_lines.append("  <tbody>")
        for i, (user, seconds) in enumerate(_sorted_rows(totals, show_method)):
            bg = "#deeaf1" if i % 2 else "#ffffff"
            TD = f"border:1px solid #9ab3c8; padding:2px 8px; background:{bg}; color:#000000"
            _priority, domain, uid, method = parsed_names[user]
            active_time = html.escape(_format_duration(seconds))
            if show_method:
                html_lines.append(
                    f"    <tr>"
                    f'<td style="{TD}">{html.escape(domain)}</td>'
                    f'<td style="{TD}">{html.escape(uid)}</td>'
                    f'<td style="{TD}">{html.escape(method)}</td>'
                    f'<td style="{TD}">{active_time}</td>'
                    f"</tr>"
                )
            else:
                html_lines.append(
                    f"    <tr>"
                    f'<td style="{TD}">{html.escape(domain)}</td>'
                    f'<td style="{TD}">{html.escape(uid)}</td>'
                    f'<td style="{TD}">{active_time}</td>'
                    f"</tr>"
                )
        html_lines.append("  </tbody>")
        html_lines.append("</table>")

    html_lines.append(f"<p><em>Timezone: {html.escape(tz_name)}</em></p>")
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
        "--time",
        metavar="HH:MM",
        help=(
            "Interpret --duration as ending at the most recent occurrence of this "
            "wall-clock time (in the given timezone) within the past 24 hours"
        ),
    )
    parser.add_argument(
        "--timezone",
        default="America/Chicago",
        metavar="TZ",
        help=(
            "Timezone for --time and all output timestamps "
            '(e.g., "America/Chicago", "MST", "+04:00"); default: America/Chicago'
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

    # Username display
    parser.add_argument(
        "--detailed-usernames",
        action="store_true",
        help='Always show the "Login method" column in the output',
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

    # Validate --time format
    if args.time is not None:
        import re
        if not re.fullmatch(r"\d{1,2}:\d{2}", args.time):
            parser.error("--time must be in HH:MM format")

    # Validate --timezone
    try:
        parse_timezone(args.timezone)
    except ValueError as e:
        parser.error(str(e))

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

        duration_td = (
            duration_seconds
            if isinstance(duration_seconds, timedelta)
            else timedelta(seconds=duration_seconds)
        )

        # Resolve timezone and compute time range
        tz = parse_timezone(args.timezone)
        start_time, end_time = compute_time_range(duration_td, args.time, tz)
        cutoff = int(start_time.timestamp())

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
            query = build_query(cutoff=cutoff, end=int(end_time.timestamp()), hub=args.hub)
            documents = list(client.query(index=args.index, query=query))
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"Error querying Elasticsearch: {e}", file=sys.stderr)
            return 1
        finally:
            client.close()

        # Aggregate active time per user
        totals = compute_activity(documents)

        # Output to stdout by default
        if not args.text_file and not args.html_file:
            print(format_output_text(totals, start_time, end_time, args.timezone, args.detailed_usernames))

        # Output to text file if specified
        if args.text_file:
            text_content = format_output_text(totals, start_time, end_time, args.timezone, args.detailed_usernames)
            args.text_file.write_text(text_content + "\n", encoding="utf-8")
            print(f"Plain text output written to: {args.text_file}", file=sys.stderr)

        # Output to HTML file if specified
        if args.html_file:
            html_content = format_output_html(totals, start_time, end_time, args.timezone, args.detailed_usernames)
            args.html_file.write_text(html_content + "\n", encoding="utf-8")
            print(f"HTML output written to: {args.html_file}", file=sys.stderr)

        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
