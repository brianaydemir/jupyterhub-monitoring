"""Command-line tool for listing new JupyterHub users."""

import argparse
import html
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytimeparse2

from app.jupyterhub_client import JupyterHubClient


def filter_new_users(
    users: list[dict], duration_seconds: int | float | timedelta
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


def format_output_text(users: list[dict], duration_str: str) -> str:
    """Format users as plain text output.

    Args:
        users: List of user dictionaries with 'name' and 'created' keys
        duration_str: Human-readable duration string (e.g., "7 days", "12h")

    Returns:
        Plain text formatted string with heading and creation dates
    """
    lines = [f"New users created in the last {duration_str}:", ""]

    if not users:
        lines.append("No new users found.")
    else:
        for user in users:
            name = user.get("name", "")
            created = user.get("created", "")
            lines.append(f"{name} (created: {created})")

    return "\n".join(lines)


def format_output_html(users: list[dict], duration_str: str) -> str:
    """Format users as HTML output suitable for email body.

    Args:
        users: List of user dictionaries with 'name' and 'created' keys
        duration_str: Human-readable duration string (e.g., "7 days", "12h")

    Returns:
        HTML formatted string (body content only) with heading and creation dates
    """
    html_lines = [f"<p>New users created in the last {html.escape(duration_str)}:</p>"]

    if not users:
        html_lines.append("<p>No new users found.</p>")
    else:
        html_lines.append("<ul>")
        for user in users:
            name = html.escape(user.get("name", ""))
            created = html.escape(user.get("created", ""))
            html_lines.append(f"  <li>{name} (created: {created})</li>")
        html_lines.append("</ul>")

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
  %(prog)s --endpoint https://hub.example.com/hub/api --api-key TOKEN --duration "7 days"
  %(prog)s --endpoint https://hub.example.com/hub/api --api-key TOKEN --duration "12h" --text-file output.txt
  %(prog)s --endpoint https://hub.example.com/hub/api --api-key TOKEN --duration "3d 6h" --html-file output.html
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
        required=True,
        help="JupyterHub API key for authentication",
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

    args = parser.parse_args()

    # Validate CA certificate exists if provided
    if args.ca_cert and not args.ca_cert.exists():
        parser.error(f"CA certificate file not found: {args.ca_cert}")

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
                api_key=args.api_key,
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

        # Output to stdout by default
        if not args.text_file and not args.html_file:
            print(format_output_text(new_users, args.duration))

        # Output to text file if specified
        if args.text_file:
            text_content = format_output_text(new_users, args.duration)
            args.text_file.write_text(text_content + "\n", encoding="utf-8")
            print(f"Plain text output written to: {args.text_file}", file=sys.stderr)

        # Output to HTML file if specified
        if args.html_file:
            html_content = format_output_html(new_users, args.duration)
            args.html_file.write_text(html_content + "\n", encoding="utf-8")
            print(f"HTML output written to: {args.html_file}", file=sys.stderr)

        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
