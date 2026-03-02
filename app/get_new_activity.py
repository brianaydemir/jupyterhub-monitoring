"""Command-line tool for reporting active server time per JupyterHub user."""

import argparse
import csv
import html
import io
import os
import smtplib
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytimeparse2

from app.cli_utils import (
    add_email_argument_group,
    add_output_argument_group,
    add_query_argument_group,
    validate_email_arguments,
    validate_query_arguments,
)
from app.elasticsearch_client import ElasticsearchClient
from app.name_utils import _trailing_domain_key, parse_name
from app.send_email import create_message as create_email_message
from app.send_email import send_email as send_email_message
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
    """Format a duration in seconds as HH:MM.

    Args:
        seconds: Duration in seconds

    Returns:
        Duration string in ``HH:MM`` format (hours may exceed 23)
    """
    total_minutes = int(seconds) // 60
    hours, minutes = divmod(total_minutes, 60)
    return f"{hours}:{minutes:02d}"


def _sorted_rows(
    totals: dict[str, float], show_method: bool
) -> list[tuple[str, float]]:
    """Return (user, seconds) pairs sorted by time descending, then by parsed name.

    When *show_method* is True, sorts by (priority, domain, id) as a tiebreaker.
    When False, sorts by (trailing_domain_key, domain, id) so trailing domains
    (e.g. orcid.org) sort last regardless of login method.
    """

    def name_key(user: str) -> tuple:
        name = parse_name(user)
        if show_method:
            return name[:3]
        return (_trailing_domain_key(name[1]), name[1], name[2])

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
        lines = [
            f"No users were active between {start_time.strftime(fmt)} and {end_time.strftime(fmt)}."
        ]
    else:
        noun = "user" if n == 1 else "users"
        lines = [f"{n} {noun} were active {range_str}:", ""]
        parsed_names = {user: parse_name(user) for user in totals}
        domain_id_pairs = [(p[1], p[2]) for p in parsed_names.values()]
        show_method = detailed_usernames or (
            len(domain_id_pairs) != len(set(domain_id_pairs))
        )
        rows = [
            (*parsed_names[user], _format_duration(seconds))
            for user, seconds in _sorted_rows(totals, show_method)
        ]
        domain_width = max(len("Institution"), max(len(r[1]) for r in rows))
        id_width = max(len("ID"), max(len(r[2]) for r in rows))
        time_width = max(len("Time (HH:MM)"), max(len(r[4]) for r in rows))
        if show_method:
            method_width = max(len("Login method"), max(len(r[3]) for r in rows))
            lines.append(
                f"{'Time (HH:MM)':>{time_width}}  {'Institution':<{domain_width}}  "
                f"{'ID':<{id_width}}  {'Login method':<{method_width}}"
            )
            lines.append(
                f"{'-' * time_width}  {'-' * domain_width}  "
                f"{'-' * id_width}  {'-' * method_width}"
            )
            for _priority, domain, uid, method, active_time in rows:
                lines.append(
                    f"{active_time:>{time_width}}  {domain:<{domain_width}}  "
                    f"{uid:<{id_width}}  {method:<{method_width}}"
                )
        else:
            lines.append(
                f"{'Time (HH:MM)':>{time_width}}  {'Institution':<{domain_width}}  "
                f"{'ID':<{id_width}}"
            )
            lines.append(f"{'-' * time_width}  {'-' * domain_width}  {'-' * id_width}")
            for _priority, domain, uid, _method, active_time in rows:
                lines.append(
                    f"{active_time:>{time_width}}  {domain:<{domain_width}}  "
                    f"{uid:<{id_width}}"
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
            f"<p>No users were active between "
            f"{html.escape(start_time.strftime(fmt))} and "
            f"{html.escape(end_time.strftime(fmt))}.</p>"
        ]
    else:
        noun = "user" if n == 1 else "users"
        html_lines = [f"<p>{n} {noun} were active {range_str}:</p>"]
        parsed_names = {user: parse_name(user) for user in totals}
        domain_id_pairs = [(p[1], p[2]) for p in parsed_names.values()]
        show_method = detailed_usernames or (
            len(domain_id_pairs) != len(set(domain_id_pairs))
        )
        TH = "text-align:left; border:1px solid #9ab3c8; padding:2px 8px; background:#a6c9e8; color:#000000"
        TH_R = "text-align:right; border:1px solid #9ab3c8; padding:2px 8px; background:#a6c9e8; color:#000000"
        html_lines.append('<table style="border-collapse:collapse">')
        html_lines.append("  <thead>")
        if show_method:
            html_lines.append(
                f"    <tr>"
                f'<th style="{TH_R}">Time (HH:MM)</th>'
                f'<th style="{TH}">Institution</th>'
                f'<th style="{TH}">ID</th>'
                f'<th style="{TH}">Login method</th>'
                f"</tr>"
            )
        else:
            html_lines.append(
                f"    <tr>"
                f'<th style="{TH_R}">Time (HH:MM)</th>'
                f'<th style="{TH}">Institution</th>'
                f'<th style="{TH}">ID</th>'
                f"</tr>"
            )
        html_lines.append("  </thead>")
        html_lines.append("  <tbody>")
        for i, (user, seconds) in enumerate(_sorted_rows(totals, show_method)):
            bg = "#e6eff4" if i % 2 else "#ffffff"
            TD = f"border:1px solid #9ab3c8; padding:2px 8px; background:{bg}; color:#000000"
            TD_R = f"text-align:right; border:1px solid #9ab3c8; padding:2px 8px; background:{bg}; color:#000000"
            _priority, domain, uid, method = parsed_names[user]
            active_time = html.escape(_format_duration(seconds))
            if show_method:
                html_lines.append(
                    f"    <tr>"
                    f'<td style="{TD_R}">{active_time}</td>'
                    f'<td style="{TD}">{html.escape(domain)}</td>'
                    f'<td style="{TD}">{html.escape(uid)}</td>'
                    f'<td style="{TD}">{html.escape(method)}</td>'
                    f"</tr>"
                )
            else:
                html_lines.append(
                    f"    <tr>"
                    f'<td style="{TD_R}">{active_time}</td>'
                    f'<td style="{TD}">{html.escape(domain)}</td>'
                    f'<td style="{TD}">{html.escape(uid)}</td>'
                    f"</tr>"
                )
        html_lines.append("  </tbody>")
        html_lines.append("</table>")

    html_lines.append(f"<p><em>Timezone: {html.escape(tz_name)}</em></p>")
    return "\n".join(html_lines)


def format_output_csv(
    totals: dict[str, float],
    start_time: datetime,
    end_time: datetime,
    tz_name: str,
    detailed_usernames: bool = False,
) -> str:
    """Format per-user activity as CSV.

    The CSV starts with the data table (header row plus one row per user),
    followed by an empty row, a summary line, and a timezone line.

    Args:
        totals: Dictionary mapping user name to total active seconds
        start_time: Start of the reporting window (timezone-aware)
        end_time: End of the reporting window (timezone-aware)
        tz_name: Timezone name for the footer
        detailed_usernames: Always show the "Login method" column

    Returns:
        CSV formatted string
    """
    fmt = "%Y-%m-%d %H:%M"
    n = len(totals)
    buf = io.StringIO()
    writer = csv.writer(buf)

    parsed_names = {user: parse_name(user) for user in totals}
    domain_id_pairs = [(p[1], p[2]) for p in parsed_names.values()]
    show_method = detailed_usernames or (
        len(domain_id_pairs) != len(set(domain_id_pairs))
    )
    if show_method:
        writer.writerow(["Time (HH:MM)", "Institution", "ID", "Login method"])
        for user, seconds in _sorted_rows(totals, show_method):
            _priority, domain, uid, method = parsed_names[user]
            writer.writerow([_format_duration(seconds), domain, uid, method])
    else:
        writer.writerow(["Time (HH:MM)", "Institution", "ID"])
        for user, seconds in _sorted_rows(totals, show_method):
            _priority, domain, uid, _method = parsed_names[user]
            writer.writerow([_format_duration(seconds), domain, uid])

    writer.writerow([])
    if n == 0:
        writer.writerow(
            [
                f"No users were active between "
                f"{start_time.strftime(fmt)} and {end_time.strftime(fmt)}."
            ]
        )
    else:
        noun = "user" if n == 1 else "users"
        writer.writerow(
            [
                f"{n} {noun} were active "
                f"from {start_time.strftime(fmt)} to {end_time.strftime(fmt)}"
            ]
        )
    writer.writerow([f"Timezone: {tz_name}"])
    return buf.getvalue()


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
    es_group = parser.add_argument_group("Elasticsearch")
    es_group.add_argument(
        "--endpoint",
        required=True,
        help="Elasticsearch API endpoint URL (e.g., https://localhost:9200)",
    )
    es_group.add_argument(
        "--api-key",
        type=Path,
        help=(
            "Path to file containing the Elasticsearch API key for authentication "
            "(or set ELASTICSEARCH_API_KEY)"
        ),
    )
    es_group.add_argument(
        "--ca-cert",
        type=Path,
        help="Path to CA certificate file for TLS verification",
    )

    # Query parameters
    query_group = add_query_argument_group(parser)
    query_group.add_argument(
        "--index",
        required=True,
        help="Name of the Elasticsearch index to query",
    )
    query_group.add_argument(
        "--hub",
        help="Filter results to documents with this meta.hub value",
    )

    # Output options
    add_output_argument_group(parser)

    # Email options
    add_email_argument_group(parser, default_subject="JupyterHub Activity Report")

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
            query = build_query(
                cutoff=cutoff, end=int(end_time.timestamp()), hub=args.hub
            )
            documents = list(client.query(index=args.index, query=query))
        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f"Error querying Elasticsearch: {e}", file=sys.stderr)
            return 1
        finally:
            client.close()

        # Aggregate active time per user
        totals = compute_activity(documents)

        # Always print the text report to stdout, bracketed by separator lines
        # so it remains visually distinct when stderr is merged into stdout
        text_content = format_output_text(
            totals, start_time, end_time, args.timezone, args.detailed_usernames
        )
        print(f"---\n{text_content}\n---")

        # Output to text file if specified
        if args.text_file:
            args.text_file.write_text(text_content + "\n", encoding="utf-8")
            print(f"Plain text output written to: {args.text_file}", file=sys.stderr)

        # Output to HTML file if specified
        if args.html_file:
            html_content = format_output_html(
                totals, start_time, end_time, args.timezone, args.detailed_usernames
            )
            args.html_file.write_text(html_content + "\n", encoding="utf-8")
            print(f"HTML output written to: {args.html_file}", file=sys.stderr)

        # Output to CSV file if specified
        if args.csv_file:
            csv_content = format_output_csv(
                totals, start_time, end_time, args.timezone, args.detailed_usernames
            )
            args.csv_file.write_text(csv_content, encoding="utf-8")
            print(f"CSV output written to: {args.csv_file}", file=sys.stderr)

        # Send email if requested
        if args.send_email:
            html_content = format_output_html(
                totals, start_time, end_time, args.timezone, args.detailed_usernames
            )
            csv_content = format_output_csv(
                totals, start_time, end_time, args.timezone, args.detailed_usernames
            )
            try:
                message = create_email_message(
                    sender_name=args.sender_name,
                    sender_email=args.sender_email,
                    recipient_name=args.recipient_name,
                    recipient_email=args.recipient_email,
                    subject=args.subject,
                    text_content=text_content,
                    html_content=html_content,
                    attachment_data=[("activity.csv", csv_content.encode("utf-8"))],
                )
                send_email_message(
                    smtp_host=args.smtp_host,
                    smtp_port=args.smtp_port,
                    use_ssl=not args.smtp_no_ssl,
                    sender_email=args.sender_email,
                    recipient_email=args.recipient_email,
                    message=message,
                )
                print("Email sent successfully", file=sys.stderr)
            except (OSError, smtplib.SMTPException) as e:
                print(f"Error sending email: {e}", file=sys.stderr)
                return 1

        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
