"""Command-line tool for listing new JupyterHub users."""

import argparse
import os
import smtplib
import sys
from collections.abc import Iterable
from datetime import datetime, timedelta

import pytimeparse2

from app.cli_utils import (
    add_email_argument_group,
    add_jupyterhub_argument_group,
    add_output_argument_group,
    add_query_argument_group,
    validate_email_arguments,
    validate_jupyterhub_arguments,
    validate_query_arguments,
)
from app.jupyterhub_client import JupyterHubClient
from app.output_formatters import format_users_csv, format_users_html, format_users_text
from app.send_email import create_message as create_email_message
from app.send_email import send_email as send_email_message
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
  %(prog)s --jupyterhub-endpoint https://hub.example.com/hub/api --jupyterhub-api-key /path/to/api-key --duration "7 days"
  %(prog)s --jupyterhub-endpoint https://hub.example.com/hub/api --jupyterhub-api-key /path/to/api-key --duration "12h" --text-file output.txt
  %(prog)s --jupyterhub-endpoint https://hub.example.com/hub/api --jupyterhub-api-key /path/to/api-key --duration "3d 6h" --html-file output.html

Environment variables:
  JUPYTERHUB_API_KEY  JupyterHub API key (used when --jupyterhub-api-key is not provided)
        """,
    )

    # JupyterHub API connection parameters
    add_jupyterhub_argument_group(parser)

    # Query parameters
    add_query_argument_group(parser)

    # Output options
    add_output_argument_group(parser)

    # Email options
    add_email_argument_group(parser, default_subject="JupyterHub New Users Report")

    args = parser.parse_args()

    # Validate JupyterHub API key and CA certificate
    validate_jupyterhub_arguments(args, parser)

    # Validate --time and --timezone
    validate_query_arguments(args, parser)

    # Validate email arguments
    validate_email_arguments(args, parser)

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
                endpoint=args.jupyterhub_endpoint,
                api_key=(
                    args.jupyterhub_api_key.read_text().strip()
                    if args.jupyterhub_api_key
                    else os.environ["JUPYTERHUB_API_KEY"]
                ),
                ca_cert=(
                    str(args.jupyterhub_ca_cert) if args.jupyterhub_ca_cert else None
                ),
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

        # Always print the text report to stdout, bracketed by separator lines
        # so it remains visually distinct when stderr is merged into stdout
        text_content = format_users_text(
            new_users,
            start_time,
            end_time,
            args.timezone,
            tz,
            strftime_fmt,
            args.detailed_usernames,
        )
        print(f"---\n{text_content}\n---")

        # Output to text file if specified
        if args.text_file:
            args.text_file.write_text(text_content + "\n", encoding="utf-8")
            print(f"Plain text output written to: {args.text_file}", file=sys.stderr)

        # Output to HTML file if specified
        if args.html_file:
            html_content = format_users_html(
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

        # Output to CSV file if specified
        if args.csv_file:
            csv_content = format_users_csv(
                new_users,
                start_time,
                end_time,
                args.timezone,
                tz,
                strftime_fmt,
                args.detailed_usernames,
            )
            args.csv_file.write_text(csv_content, encoding="utf-8")
            print(f"CSV output written to: {args.csv_file}", file=sys.stderr)

        # Send email if requested
        if args.send_email:
            html_content = format_users_html(
                new_users,
                start_time,
                end_time,
                args.timezone,
                tz,
                strftime_fmt,
                args.detailed_usernames,
            )
            csv_content = format_users_csv(
                new_users,
                start_time,
                end_time,
                args.timezone,
                tz,
                strftime_fmt,
                args.detailed_usernames,
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
                    attachment_data=[("new-users.csv", csv_content.encode("utf-8"))],
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
