"""Command-line tool for reporting active server time per JupyterHub user."""

import argparse
import sys
from typing import Final

from app.cli.arguments import add_hub_argument
from app.cli.factories import (
    compute_report_time_range,
    make_es_client,
    make_output_targets,
)
from app.cli.report_arguments import (
    ELASTICSEARCH_SOURCE,
    configure_report_parser,
    validate_report_arguments,
)
from app.cli.runtime import run_command
from app.core.errors import DataShapeError
from app.core.time_utils import DATETIME_FMT
from app.reports.builders import build_activity_report
from app.reports.delivery import deliver_report
from app.usage.activity import (
    DocumentTally,
    SkipReason,
    compute_activity,
    summarize_activity,
    tally_documents,
)
from app.usage.snapshots import build_query

# The data this report draws on. Shared by the parser and the validator
# so that the two cannot disagree about which arguments exist.
_SOURCES: Final = (ELASTICSEARCH_SOURCE,)

# The fixed part of the default email subject, which
# :func:`~app.cli.factories.make_output_targets` completes with the
# reporting window. Shared by the parser, which names it in the help for
# ``--subject``, and the run that fills it in, so that the default the
# help describes is the one a message actually carries.
_SUBJECT_PREFIX: Final = "JupyterHub Activity Report"


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for this command.

    There is no ``--date-format``: the ``Started`` column identifies a
    session, so a bare date would render a user's separate same-day runs
    of one server as rows differing only in a duration. The timestamps
    are always :data:`~app.core.time_utils.DATETIME_FMT`, as they were
    before the option reached this command.
    """
    parser = argparse.ArgumentParser(
        description="Report active server time per JupyterHub user from Elasticsearch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --es-endpoint https://es.example.com:9200 \\
    --es-api-key /path/to/key --es-index servers \\
    --duration "7 days"
  %(prog)s --es-endpoint https://es.example.com:9200 \\
    --es-api-key /path/to/key --es-index servers \\
    --duration "24h" --hub myhub
  %(prog)s --es-endpoint https://es.example.com:9200 \\
    --es-api-key /path/to/key --es-index servers \\
    --report-start "July 1 2026" --report-end "July 8 2026"

Environment variables:
  ELASTICSEARCH_API_KEY  API key (when --es-api-key is
                         not provided)
        """,
    )

    query_group = configure_report_parser(
        parser,
        sources=_SOURCES,
        subject_prefix=_SUBJECT_PREFIX,
        include_date_format=False,
        include_anonymize=True,
    )
    add_hub_argument(query_group)
    return parser


def _validate_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Run post-parse argument validation."""
    validate_report_arguments(args, parser, sources=_SOURCES)


def _format_document_tally(tally: DocumentTally) -> str:
    """Render the retrieved/counted/skipped diagnostic for stderr.

    A run that retrieved nothing says only that, since it has nothing to
    have counted or skipped. Skips are broken out by reason, which is
    what separates the ways a zero report arises: an idle hub, a wrong
    ``--es-index``, and a ``--hub`` matching no document all retrieve
    nothing at all, whereas documents the counting rule rejects were
    retrieved and then discarded, and only a skip count says so.
    """
    noun = "document" if tally.retrieved == 1 else "documents"
    retrieved = f"Retrieved {tally.retrieved} {noun}"
    if not tally.retrieved:
        return f"{retrieved}."
    if not tally.skipped:
        return f"{retrieved}; counted {tally.counted}."
    reasons = ", ".join(
        f"{reason.value}: {tally.skipped[reason]}"
        for reason in SkipReason
        if reason in tally.skipped
    )
    skipped = sum(tally.skipped.values())
    return f"{retrieved}; counted {tally.counted}, skipped {skipped} ({reasons})."


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    The document tally goes to stderr, beside the reporting window that
    :func:`~app.cli.factories.compute_report_time_range` echoes there, so
    the report :func:`~app.reports.delivery.deliver_report` writes to
    stdout stays safe to pipe.

    Raises:
        DataShapeError: If the reporting window is not timezone-aware.
        ExternalServiceError: If Elasticsearch querying fails.
    """
    start_time, end_time = compute_report_time_range(args)
    cutoff = int(start_time.timestamp())
    tz = start_time.tzinfo
    if tz is None:
        raise DataShapeError("start_time must be timezone-aware")

    with make_es_client(args) as client:
        query = build_query(
            cutoff=cutoff,
            end=int(end_time.timestamp()),
            hub=args.hub,
        )
        documents = list(client.query(index=args.es_index, query=query))

    print(_format_document_tally(tally_documents(documents)), file=sys.stderr)

    sessions = compute_activity(documents)
    summary = summarize_activity(sessions, documents, tz, start_time, end_time)
    report = build_activity_report(
        sessions=sessions,
        summary=summary,
        start_time=start_time,
        end_time=end_time,
        strftime_fmt=DATETIME_FMT,
        detailed_usernames=args.detailed_usernames,
        anonymize=args.anonymize,
    )
    targets = make_output_targets(
        args,
        subject_prefix=_SUBJECT_PREFIX,
        window=(start_time, end_time),
    )
    return deliver_report(targets, report)


def main() -> int:
    """Main entry point for the get-new-activity script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
