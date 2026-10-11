"""Command-line tool for listing new JupyterHub users."""

import argparse
import sys
from datetime import datetime, timezone
from typing import Final

from app.cli.arguments import add_hub_argument
from app.cli.factories import (
    compute_report_time_range,
    get_strftime_fmt,
    make_es_client,
    make_jupyterhub_client,
    make_output_targets,
)
from app.cli.report_arguments import (
    JUPYTERHUB_SOURCE,
    OPTIONAL_ELASTICSEARCH_SOURCE,
    configure_report_parser,
    validate_report_arguments,
)
from app.cli.runtime import run_command
from app.reports.builders import build_new_users_report
from app.reports.delivery import deliver_report
from app.usage.new_users import UserTally, compute_first_server, filter_new_users, tally_users
from app.usage.snapshots import build_query, select_hub

# The data this report draws on.
# Shared by the parser and the validator
# so that the two cannot disagree about which arguments exist.
_SOURCES: Final = (JUPYTERHUB_SOURCE, OPTIONAL_ELASTICSEARCH_SOURCE)

# The fixed part of the default email subject,
# which the run completes with the reporting window.
# Shared with the parser, whose help for ``--subject`` names it.
_SUBJECT_PREFIX: Final = "JupyterHub New Users Report"


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
    --report-start "2 weeks ago" --report-end "yesterday" --whole-days

Elasticsearch:
  Optional. Pass --es-endpoint and --es-index to also
  report when each new user first started a server.

Environment variables:
  JUPYTERHUB_API_KEY     API key (when --jupyterhub-api-key
                         is not provided)
  ELASTICSEARCH_API_KEY  API key (when --es-api-key is not
                         provided)
        """,
    )
    query_group = configure_report_parser(
        parser,
        sources=_SOURCES,
        subject_prefix=_SUBJECT_PREFIX,
        include_anonymize=True,
    )
    add_hub_argument(query_group)
    return parser


def _validate_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Run post-parse argument validation.

    ``--hub`` scopes the Elasticsearch lookup alone,
    so passing it without ``--es-endpoint`` is an error rather than a filter
    that silently does nothing.
    """
    validate_report_arguments(args, parser, sources=_SOURCES)
    if args.hub is not None and args.es_endpoint is None:
        parser.error("--hub requires --es-endpoint")


def _format_user_tally(tally: UserTally) -> str:
    """Render the retrieved/new user diagnostic for stderr.

    The total is what tells a quiet window apart from an endpoint or token
    that reached a hub with few or no users on it.
    Users with no creation time are mentioned only when there are some,
    as ``get-new-activity`` mentions skipped documents.
    """
    noun = "user" if tally.retrieved == 1 else "users"
    retrieved = f"Retrieved {tally.retrieved} {noun}"
    if not tally.retrieved:
        return f"{retrieved}."
    created = f"{tally.in_window} created in the reporting window"
    if not tally.undated:
        return f"{retrieved}; {created}."
    return f"{retrieved}; {created}, {tally.undated} with no creation time."


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Elasticsearch is consulted for first-server times
    only when ``--es-endpoint`` was given.
    The user tally leads the report, and also goes to stderr,
    where a scheduled run's log keeps it apart from the report on stdout.

    Raises:
        ExternalServiceError: If listing users or querying Elasticsearch
            fails.
    """
    start_time, end_time = compute_report_time_range(args)

    users = list(make_jupyterhub_client(args).list_users())

    new_users = filter_new_users(users, start_time, end_time)
    tally = tally_users(users, start_time, end_time)

    print(_format_user_tally(tally), file=sys.stderr)

    first_server: dict[str, int] | None = None
    if args.es_endpoint is not None:
        first_server = _query_first_server(args, start_time) if new_users else {}

    strftime_fmt = get_strftime_fmt(args)

    report = build_new_users_report(
        users=new_users,
        tally=tally,
        start_time=start_time,
        end_time=end_time,
        strftime_fmt=strftime_fmt,
        detailed_usernames=args.detailed_usernames,
        first_server=first_server,
        anonymize=args.anonymize,
    )
    targets = make_output_targets(
        args,
        subject_prefix=_SUBJECT_PREFIX,
        window=(start_time, end_time),
    )
    return deliver_report(targets, report)


def _query_first_server(
    args: argparse.Namespace,
    start_time: datetime,
) -> dict[str, int]:
    """Query Elasticsearch for each new user's first server-start time.

    Searches from *start_time* through now:
    a user created inside the reporting window cannot have started a server
    before being created, so this captures their true first start
    even if it occurred after the window closed.
    A snapshot taken before *start_time* is not visible.

    ``--hub`` scopes the search the same way it does for ``get-new-activity``,
    so on a multi-hub index the column reports a user's first server
    on that hub rather than anywhere in the index.

    Raises:
        ExternalServiceError: If Elasticsearch querying fails.
    """
    now = datetime.now(timezone.utc)
    with make_es_client(args) as client:
        query = build_query(
            cutoff=int(start_time.timestamp()),
            end=int(now.timestamp()),
            hub=args.hub,
        )
        documents = select_hub(client.query(index=args.es_index, query=query), args.hub)
        return compute_first_server(documents)


def main() -> int:
    """Main entry point for the get-new-users script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
