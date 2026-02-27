"""Command-line tool for listing new JupyterHub users."""

import argparse
import html
import os
import sys
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from pathlib import Path

import humanize
import pytimeparse2

from app.jupyterhub_client import JupyterHubClient


def filter_new_users(
    users: Iterable[dict], duration_seconds: int | float | timedelta
) -> list[dict]:
    """Filter users created within the specified duration.

    Args:
        users: List of user dictionaries from JupyterHub API
        duration_seconds: Duration to look back from now (int/float as seconds,
                          or timedelta object)

    Returns:
        List of user dictionaries (with 'name' and 'created' keys) that were
        created within the specified duration
    """
    now = datetime.now(timezone.utc)

    # Convert duration to seconds if it's a timedelta
    if isinstance(duration_seconds, timedelta):
        seconds = duration_seconds.total_seconds()
    else:
        seconds = float(duration_seconds)

    cutoff_time = now - timedelta(seconds=seconds)

    new_users = []
    for user in users:
        # Get the user's creation timestamp
        created_str = user.get("created")
        if not created_str:
            continue

        # Parse the ISO 8601 timestamp from JupyterHub
        try:
            created_dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            if created_dt >= cutoff_time:
                new_users.append({"name": user.get("name", ""), "created": created_str})
        except ValueError, AttributeError:
            # Skip users with invalid timestamps
            continue

    return new_users


def _format_created(created_str: str, strftime_fmt: str) -> str:
    """Reformat a JupyterHub ISO 8601 creation timestamp.

    Args:
        created_str: ISO 8601 timestamp string from JupyterHub
        strftime_fmt: strftime format string to apply

    Returns:
        Formatted timestamp string, or the original string if parsing fails
    """
    try:
        dt = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        return dt.strftime(strftime_fmt)
    except ValueError, AttributeError:
        return created_str


def format_output_text(users: list[dict], duration_str: str, strftime_fmt: str) -> str:
    """Format users as plain text output.

    Args:
        users: List of user dictionaries with 'name' and 'created' keys
        duration_str: Human-readable duration string (e.g., "7 days", "12h")
        strftime_fmt: strftime format string for creation timestamps

    Returns:
        Plain text formatted string with a two-column table of name and creation date
    """
    n = len(users)
    if n == 0:
        lines = [f"No new users created in the last {duration_str}."]
    else:
        noun = "user" if n == 1 else "users"
        lines = [f"{n} new {noun} created in the last {duration_str}:", ""]
        rows = sorted(
            [
                (
                    user.get("name", ""),
                    _format_created(user.get("created", ""), strftime_fmt),
                )
                for user in users
            ],
            key=lambda r: (r[1], r[0]),
        )
        name_width = max(len("Name"), max(len(r[0]) for r in rows))
        created_width = max(len("Created"), max(len(r[1]) for r in rows))
        lines.append(f"{'Name':<{name_width}}  {'Created':<{created_width}}")
        lines.append(f"{'-' * name_width}  {'-' * created_width}")
        for name, created in rows:
            lines.append(f"{name:<{name_width}}  {created:<{created_width}}")

    return "\n".join(lines)


def format_output_html(users: list[dict], duration_str: str, strftime_fmt: str) -> str:
    """Format users as HTML output suitable for email body.

    Args:
        users: List of user dictionaries with 'name' and 'created' keys
        duration_str: Human-readable duration string (e.g., "7 days", "12h")
        strftime_fmt: strftime format string for creation timestamps

    Returns:
        HTML formatted string (body content only) with a table of name and creation date
    """
    n = len(users)
    esc_duration = html.escape(duration_str)

    if not users:
        html_lines = [f"<p>No new users created in the last {esc_duration}.</p>"]
    else:
        noun = "user" if n == 1 else "users"
        html_lines = [f"<p>{n} new {noun} created in the last {esc_duration}:</p>"]
        html_lines.append("<table>")
        html_lines.append("  <thead>")
        html_lines.append(
            '    <tr><th style="text-align:left">Name</th><th style="text-align:left">Created</th></tr>'
        )
        html_lines.append("  </thead>")
        html_lines.append("  <tbody>")
        for user in sorted(
            users,
            key=lambda u: (
                _format_created(u.get("created", ""), strftime_fmt),
                u.get("name", ""),
            ),
        ):
            name = html.escape(user.get("name", ""))
            created = html.escape(
                _format_created(user.get("created", ""), strftime_fmt)
            )
            html_lines.append(f"    <tr><td>{name}</td><td>{created}</td></tr>")
        html_lines.append("  </tbody>")
        html_lines.append("</table>")

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

        # Filter for new users within the specified duration
        new_users = filter_new_users(users, duration_seconds)

        # Determine the strftime format from the --date-format choice
        strftime_fmt = "%Y-%m-%d" if args.date_format == "date" else "%Y-%m-%d %H:%M"

        human_duration = humanize.naturaldelta(
            duration_seconds
            if isinstance(duration_seconds, timedelta)
            else timedelta(seconds=float(duration_seconds))
        )

        # Output to stdout by default
        if not args.text_file and not args.html_file:
            print(format_output_text(new_users, human_duration, strftime_fmt))

        # Output to text file if specified
        if args.text_file:
            text_content = format_output_text(new_users, human_duration, strftime_fmt)
            args.text_file.write_text(text_content + "\n", encoding="utf-8")
            print(f"Plain text output written to: {args.text_file}", file=sys.stderr)

        # Output to HTML file if specified
        if args.html_file:
            html_content = format_output_html(new_users, human_duration, strftime_fmt)
            args.html_file.write_text(html_content + "\n", encoding="utf-8")
            print(f"HTML output written to: {args.html_file}", file=sys.stderr)

        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
