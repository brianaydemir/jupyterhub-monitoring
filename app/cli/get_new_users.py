"""Command-line tool for listing new JupyterHub users."""

import argparse
import sys
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from app.cli.runtime import run_command
from app.cli.utils import (
    compute_report_time_range,
    configure_report_parser,
    get_strftime_fmt,
    make_jupyterhub_client,
    validate_report_arguments,
)
from app.core.errors import ExternalServiceError
from app.reports.builders import build_new_users_report
from app.reports.delivery import deliver_report


def filter_new_users(
    users: Iterable[dict[str, Any]],
    cutoff_time: datetime,
    end_time: datetime,
) -> list[dict[str, Any]]:
    """Filter users created within [*cutoff_time*, *end_time*]."""
    new_users: list[dict[str, Any]] = []
    for user in users:
        created_str = user.get("created")
        if not created_str:
            continue

        try:
            created_dt = datetime.fromisoformat(created_str)
            if created_dt.tzinfo is None:
                created_dt = created_dt.replace(tzinfo=timezone.utc)
            if cutoff_time <= created_dt <= end_time:
                new_users.append(
                    {
                        "name": user.get("name", ""),
                        "created": created_str,
                    }
                )
        except (ValueError, TypeError, AttributeError):
            continue

    return new_users


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for this command."""
    parser = argparse.ArgumentParser(
        description="List JupyterHub users created within a specified time period",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s \\
    --jupyterhub-endpoint https://hub.example.com/hub/api \\
    --jupyterhub-api-key /path/to/key --duration "7 days"
  %(prog)s \\
    --jupyterhub-endpoint https://hub.example.com/hub/api \\
    --jupyterhub-api-key /path/to/key --duration "12h" \\
    --text-file output.txt
  %(prog)s \\
    --jupyterhub-endpoint https://hub.example.com/hub/api \\
    --jupyterhub-api-key /path/to/key \\
    --report-start "2 weeks ago" --report-end "yesterday"

Environment variables:
  JUPYTERHUB_API_KEY  API key (when --jupyterhub-api-key
                      is not provided)
        """,
    )
    configure_report_parser(
        parser,
        source="jupyterhub",
        default_subject="JupyterHub New Users Report",
    )
    return parser


def _validate_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Run post-parse argument validation."""
    validate_report_arguments(args, parser, source="jupyterhub")


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Raises:
        ExternalServiceError: If user listing fails.
    """
    start_time, end_time = compute_report_time_range(args)

    try:
        users = list(make_jupyterhub_client(args).list_users())
    except Exception as e:
        raise ExternalServiceError(f"Listing users failed: {e}") from e

    new_users = filter_new_users(users, start_time, end_time)

    strftime_fmt = get_strftime_fmt(args)

    report = build_new_users_report(
        users=new_users,
        start_time=start_time,
        end_time=end_time,
        tz_name=args.timezone,
        strftime_fmt=strftime_fmt,
        detailed_usernames=args.detailed_usernames,
    )
    return deliver_report(args, report)


def main() -> int:
    """Main entry point for the get-new-users script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
