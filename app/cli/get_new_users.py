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
from app.usage.new_users import compute_first_server, filter_new_users
from app.usage.snapshots import build_query

# The data this report draws on. Shared by the parser and the validator
# so that the two cannot disagree about which arguments exist.
_SOURCES: Final = (JUPYTERHUB_SOURCE, OPTIONAL_ELASTICSEARCH_SOURCE)

# The fixed part of the default email subject, which
# :func:`~app.cli.factories.make_output_targets` completes with the
# reporting window. Shared by the parser, which names it in the help for
# ``--subject``, and the run that fills it in, so that the default the
# help describes is the one a message actually carries.
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
  Optional. Pass --es-endpoint and --es-index to add a
  "First server" column reporting when each new user
  first started a server. Without them the column is
  omitted.

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

    ``--hub`` scopes the Elasticsearch lookup alone, so passing it
    without ``--es-endpoint`` is an error rather than a filter that
    silently does nothing. The check lives here rather than in
    :func:`~app.cli.report_arguments.validate_report_arguments` because
    ``get-new-activity`` requires Elasticsearch outright, leaving it
    nothing to catch there.
    """
    validate_report_arguments(args, parser, sources=_SOURCES)
    if args.hub is not None and args.es_endpoint is None:
        parser.error("--hub requires --es-endpoint")


def _format_user_tally(retrieved: int, created: int) -> str:
    """Render the retrieved/new user diagnostic for stderr.

    A run that retrieved no users at all says only that, since none of
    them can then have been created in the window. The pair is what
    separates the failure that matters here, an endpoint or token that
    returns few or no users, from a window that was merely quiet; a
    mistaken endpoint that answers at all is otherwise a report of no
    new users. This is the counterpart of ``get-new-activity``'s
    document tally, and stops at the same boundary: what became of the
    new users is the report's own Adoption section, not this line.
    """
    noun = "user" if retrieved == 1 else "users"
    if not retrieved:
        return f"Retrieved {retrieved} {noun}."
    return f"Retrieved {retrieved} {noun}; {created} created in the reporting window."


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Elasticsearch is consulted for first-server times only when
    ``--es-endpoint`` was given.

    The user tally goes to stderr, beside the reporting window that
    :func:`~app.cli.factories.compute_report_time_range` echoes there, so
    the report :func:`~app.reports.delivery.deliver_report` writes to
    stdout stays safe to pipe. Both counts are lengths of lists this
    function already holds, so no counting rule is repeated to produce
    them.

    Raises:
        ExternalServiceError: If listing users or querying Elasticsearch
            fails.
    """
    start_time, end_time = compute_report_time_range(args)

    users = list(make_jupyterhub_client(args).list_users())

    new_users = filter_new_users(users, start_time, end_time)

    print(_format_user_tally(len(users), len(new_users)), file=sys.stderr)

    first_server: dict[str, int] | None = None
    if args.es_endpoint is not None:
        first_server = _query_first_server(args, start_time) if new_users else {}

    strftime_fmt = get_strftime_fmt(args)

    report = build_new_users_report(
        users=new_users,
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

    Searches from *start_time* through now: a user created inside the
    reporting window cannot have started a server before being created, so
    this captures their true first start even if it occurred after the
    window closed. A server started before *start_time* is not visible.

    ``--hub`` scopes the search the same way it does for
    ``get-new-activity``, so on a multi-hub index the column reports a
    user's first server on that hub rather than anywhere in the index.

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
        return compute_first_server(client.query(index=args.es_index, query=query))


def main() -> int:
    """Main entry point for the get-new-users script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
