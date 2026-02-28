"""Command-line tool for listing new JupyterHub users."""

import argparse
import html
import os
import re
import sys
from collections.abc import Iterable
from datetime import datetime, timedelta, tzinfo
from pathlib import Path

import pytimeparse2

from app.jupyterhub_client import JupyterHubClient
from app.name_utils import _trailing_domain_key, parse_name
from app.time_utils import compute_time_range, parse_timezone


def filter_new_users(
    users: Iterable[dict], cutoff_time: datetime, end_time: datetime
) -> list[dict]:
    """Filter users created within [*cutoff_time*, *end_time*].

    Args:
        users: List of user dictionaries from JupyterHub API
        cutoff_time: Timezone-aware datetime; only users created at or after
            this moment are returned
        end_time: Timezone-aware datetime; only users created at or before
            this moment are returned

    Returns:
        List of user dictionaries (with 'name' and 'created' keys)
    """
    new_users = []
    for user in users:
        created_str = user.get("created")
        if not created_str:
            continue

        try:
            created_dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            if cutoff_time <= created_dt <= end_time:
                new_users.append({"name": user.get("name", ""), "created": created_str})
        except ValueError, AttributeError:
            continue

    return new_users


def _format_created(created_str: str, strftime_fmt: str, tz: tzinfo) -> str:
    """Reformat a JupyterHub ISO 8601 creation timestamp in a given timezone.

    Args:
        created_str: ISO 8601 timestamp string from JupyterHub
        strftime_fmt: strftime format string to apply
        tz: Timezone to convert the timestamp into before formatting

    Returns:
        Formatted timestamp string, or the original string if parsing fails
    """
    try:
        dt = datetime.fromisoformat(created_str.replace("Z", "+00:00")).astimezone(tz)
        return dt.strftime(strftime_fmt)
    except ValueError, AttributeError:
        return created_str


def format_output_text(
    users: list[dict],
    start_time: datetime,
    end_time: datetime,
    tz_name: str,
    tz: tzinfo,
    strftime_fmt: str,
    detailed_usernames: bool = False,
) -> str:
    """Format users as plain text output.

    Args:
        users: List of user dictionaries with 'name' and 'created' keys
        start_time: Start of the reporting window (timezone-aware)
        end_time: End of the reporting window (timezone-aware)
        tz_name: Timezone name for the footnote
        tz: Timezone for converting creation timestamps
        strftime_fmt: strftime format string for creation timestamps
        detailed_usernames: Always show the "Login method" column

    Returns:
        Plain text formatted string with a table of creation date and name columns
    """
    fmt = "%Y-%m-%d %H:%M"
    range_str = f"from {start_time.strftime(fmt)} to {end_time.strftime(fmt)}"
    n = len(users)
    if n == 0:
        lines = [
            f"No new users created between "
            f"{start_time.strftime(fmt)} and {end_time.strftime(fmt)}."
        ]
    else:
        noun = "user" if n == 1 else "users"
        lines = [f"{n} new {noun} created {range_str}:", ""]
        parsed = [
            (
                _format_created(user.get("created", ""), strftime_fmt, tz),
                *parse_name(user.get("name", "")),
            )
            for user in users
        ]
        # Determine whether the "Login method" column is needed
        domain_id_pairs = [(r[2], r[3]) for r in parsed]
        show_method = detailed_usernames or (
            len(domain_id_pairs) != len(set(domain_id_pairs))
        )
        if show_method:
            rows = sorted(parsed, key=lambda r: (r[0], r[1], r[2], r[3]))
        else:
            rows = sorted(
                parsed,
                key=lambda r: (r[0], _trailing_domain_key(r[2]), r[2], r[3]),
            )
        created_width = max(len("Created"), max(len(r[0]) for r in rows))
        domain_width = max(len("Institution"), max(len(r[2]) for r in rows))
        id_width = max(len("ID"), max(len(r[3]) for r in rows))
        if show_method:
            method_width = max(len("Login method"), max(len(r[4]) for r in rows))
            lines.append(
                f"{'Created':<{created_width}}  {'Institution':<{domain_width}}  "
                f"{'ID':<{id_width}}  {'Login method':<{method_width}}"
            )
            lines.append(
                f"{'-' * created_width}  {'-' * domain_width}  "
                f"{'-' * id_width}  {'-' * method_width}"
            )
            for created, _priority, domain, uid, method in rows:
                lines.append(
                    f"{created:<{created_width}}  {domain:<{domain_width}}  "
                    f"{uid:<{id_width}}  {method:<{method_width}}"
                )
        else:
            lines.append(
                f"{'Created':<{created_width}}  {'Institution':<{domain_width}}  "
                f"{'ID':<{id_width}}"
            )
            lines.append(
                f"{'-' * created_width}  {'-' * domain_width}  {'-' * id_width}"
            )
            for created, _priority, domain, uid, _method in rows:
                lines.append(
                    f"{created:<{created_width}}  {domain:<{domain_width}}  "
                    f"{uid:<{id_width}}"
                )

    lines.append("")
    lines.append(f"Timezone: {tz_name}")
    return "\n".join(lines)


def format_output_html(
    users: list[dict],
    start_time: datetime,
    end_time: datetime,
    tz_name: str,
    tz: tzinfo,
    strftime_fmt: str,
    detailed_usernames: bool = False,
) -> str:
    """Format users as HTML output suitable for email body.

    Args:
        users: List of user dictionaries with 'name' and 'created' keys
        start_time: Start of the reporting window (timezone-aware)
        end_time: End of the reporting window (timezone-aware)
        tz_name: Timezone name for the footnote
        tz: Timezone for converting creation timestamps
        strftime_fmt: strftime format string for creation timestamps
        detailed_usernames: Always show the "Login method" column

    Returns:
        HTML formatted string (body content only) with a table of creation date and name columns
    """
    fmt = "%Y-%m-%d %H:%M"
    range_str = f"from {html.escape(start_time.strftime(fmt))} to {html.escape(end_time.strftime(fmt))}"
    n = len(users)

    if not users:
        html_lines = [
            f"<p>No new users created between "
            f"{html.escape(start_time.strftime(fmt))} and "
            f"{html.escape(end_time.strftime(fmt))}.</p>"
        ]
    else:
        noun = "user" if n == 1 else "users"
        html_lines = [f"<p>{n} new {noun} created {range_str}:</p>"]
        parsed = [parse_name(user.get("name", "")) for user in users]
        domain_id_pairs = [(p[1], p[2]) for p in parsed]
        show_method = detailed_usernames or (
            len(domain_id_pairs) != len(set(domain_id_pairs))
        )
        TH = "text-align:left; border:1px solid #9ab3c8; padding:2px 8px; background:#a6c9e8; color:#000000"
        html_lines.append('<table style="border-collapse:collapse">')
        html_lines.append("  <thead>")
        if show_method:
            html_lines.append(
                f"    <tr>"
                f'<th style="{TH}">Created</th>'
                f'<th style="{TH}">Institution</th>'
                f'<th style="{TH}">ID</th>'
                f'<th style="{TH}">Login method</th>'
                f"</tr>"
            )
        else:
            html_lines.append(
                f"    <tr>"
                f'<th style="{TH}">Created</th>'
                f'<th style="{TH}">Institution</th>'
                f'<th style="{TH}">ID</th>'
                f"</tr>"
            )
        html_lines.append("  </thead>")
        html_lines.append("  <tbody>")

        def sort_key(u: dict) -> tuple:
            created = _format_created(u.get("created", ""), strftime_fmt, tz)
            name = parse_name(u.get("name", ""))
            if show_method:
                return (created, *name[:3])
            return (created, _trailing_domain_key(name[1]), name[1], name[2])

        for i, user in enumerate(sorted(users, key=sort_key)):
            bg = "#e6eff4" if i % 2 else "#ffffff"
            TD = f"border:1px solid #9ab3c8; padding:2px 8px; background:{bg}; color:#000000"
            created = html.escape(
                _format_created(user.get("created", ""), strftime_fmt, tz)
            )
            _priority, domain, uid, method = parse_name(user.get("name", ""))
            if show_method:
                html_lines.append(
                    f"    <tr>"
                    f'<td style="{TD}">{created}</td>'
                    f'<td style="{TD}">{html.escape(domain)}</td>'
                    f'<td style="{TD}">{html.escape(uid)}</td>'
                    f'<td style="{TD}">{html.escape(method)}</td>'
                    f"</tr>"
                )
            else:
                html_lines.append(
                    f"    <tr>"
                    f'<td style="{TD}">{created}</td>'
                    f'<td style="{TD}">{html.escape(domain)}</td>'
                    f'<td style="{TD}">{html.escape(uid)}</td>'
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
        description="List JupyterHub users created within a specified time period",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --endpoint https://hub.example.com/hub/api --api-key /path/to/api-key --duration "7 days"
  %(prog)s --endpoint https://hub.example.com/hub/api --api-key /path/to/api-key --duration "12h" --text-file output.txt
  %(prog)s --endpoint https://hub.example.com/hub/api --api-key /path/to/api-key --duration "3d 6h" --html-file output.html

Environment variables:
  JUPYTERHUB_API_KEY  JupyterHub API key (used when --api-key is not provided)
        """,
    )

    # JupyterHub API connection parameters
    parser.add_argument(
        "--endpoint",
        required=True,
        help="JupyterHub API endpoint URL (e.g., https://hub.example.com/hub/api)",
    )
    parser.add_argument(
        "--api-key",
        type=Path,
        help=(
            "Path to file containing the JupyterHub API key for authentication "
            "(or set JUPYTERHUB_API_KEY)"
        ),
    )
    parser.add_argument(
        "--ca-cert",
        type=Path,
        help="Path to CA certificate file for TLS verification",
    )

    # Duration parameter
    parser.add_argument(
        "--duration",
        required=True,
        help=(
            'Time duration to look back (e.g., "30 seconds", "15 min", '
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

    # Timestamp format
    parser.add_argument(
        "--date-format",
        choices=["date", "datetime"],
        default="date",
        help=(
            "Format for creation timestamps: 'date' (default, YYYY-MM-DD) or "
            "'datetime' (YYYY-MM-DD HH:MM)"
        ),
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
    elif not os.environ.get("JUPYTERHUB_API_KEY"):
        parser.error(
            "--api-key or the JUPYTERHUB_API_KEY environment variable is required"
        )

    # Validate --time format
    if args.time is not None:
        if not re.fullmatch(r"\d{1,2}:\d{2}", args.time):
            parser.error("--time must be in HH:MM format")

    # Validate --timezone
    try:
        parse_timezone(args.timezone)
    except ValueError as e:
        parser.error(str(e))

    return args


def main() -> int:
    """Main entry point for the get new users script.

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
            else timedelta(seconds=float(duration_seconds))
        )

        # Resolve timezone and compute time range
        tz = parse_timezone(args.timezone)
        start_time, end_time = compute_time_range(duration_td, args.time, tz)

        # Initialize the JupyterHub client
        try:
            client = JupyterHubClient(
                endpoint=args.endpoint,
                api_key=(
                    args.api_key.read_text().strip()
                    if args.api_key
                    else os.environ["JUPYTERHUB_API_KEY"]
                ),
                ca_cert=str(args.ca_cert) if args.ca_cert else None,
            )
        except ConnectionError as e:
            print(f"Error connecting to JupyterHub: {e}", file=sys.stderr)
            return 1

        # Get all users from JupyterHub
        try:
            users = client.list_users()
        except ConnectionError as e:
            print(f"Error listing users: {e}", file=sys.stderr)
            return 1

        # Filter for new users within the computed time range
        new_users = filter_new_users(users, start_time, end_time)

        # Determine the strftime format from the --date-format choice
        strftime_fmt = "%Y-%m-%d" if args.date_format == "date" else "%Y-%m-%d %H:%M"

        # Output to stdout by default
        if not args.text_file and not args.html_file:
            print(
                format_output_text(
                    new_users,
                    start_time,
                    end_time,
                    args.timezone,
                    tz,
                    strftime_fmt,
                    args.detailed_usernames,
                )
            )

        # Output to text file if specified
        if args.text_file:
            text_content = format_output_text(
                new_users,
                start_time,
                end_time,
                args.timezone,
                tz,
                strftime_fmt,
                args.detailed_usernames,
            )
            args.text_file.write_text(text_content + "\n", encoding="utf-8")
            print(f"Plain text output written to: {args.text_file}", file=sys.stderr)

        # Output to HTML file if specified
        if args.html_file:
            html_content = format_output_html(
                new_users,
                start_time,
                end_time,
                args.timezone,
                tz,
                strftime_fmt,
                args.detailed_usernames,
            )
            args.html_file.write_text(html_content + "\n", encoding="utf-8")
            print(f"HTML output written to: {args.html_file}", file=sys.stderr)

        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
