"""Command-line tool for pushing JupyterHub servers to Elasticsearch."""

import argparse
import sys

from app.cli.arguments import add_es_argument_group, add_jupyterhub_argument_group
from app.cli.factories import make_es_client, make_jupyterhub_client
from app.cli.runtime import run_command
from app.cli.validation import (
    positive_int_or_error,
    reject_es_flags_without_endpoint,
    validate_es_arguments,
    validate_jupyterhub_arguments,
)
from app.core.errors import DataShapeError
from app.core.time_utils import parse_duration
from app.usage.push import ElasticsearchSink, PrintingSink, parse_metadata, push_servers


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for this command."""
    parser = argparse.ArgumentParser(
        description="Push JupyterHub servers to Elasticsearch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s \\
    --jupyterhub-endpoint https://hub.example.com/hub/api \\
    --jupyterhub-api-key /path/to/key \\
    --es-endpoint https://es.example.com:9200 \\
    --es-api-key /path/to/key --es-index servers \\
    --interval 5m
  %(prog)s \\
    --jupyterhub-endpoint https://hub.example.com/hub/api \\
    --jupyterhub-api-key /path/to/key \\
    --es-endpoint https://es.example.com:9200 \\
    --es-api-key /path/to/key --es-index servers \\
    --interval 5m --metadata hub=production
  %(prog)s \\
    --jupyterhub-endpoint https://hub.example.com/hub/api \\
    --jupyterhub-api-key /path/to/key \\
    --interval 5m --debug --limit 5

Interval:
  --interval must match how often this command actually
  runs. It is stored as meta.interval, and get-new-activity
  adds it up to compute per-user uptime.

Environment variables:
  JUPYTERHUB_API_KEY     API key (when --jupyterhub-api-key
                         is not provided)
  ELASTICSEARCH_API_KEY  API key (when --es-api-key is not
                         provided)
        """,
    )

    add_jupyterhub_argument_group(parser)
    add_es_argument_group(parser, required=False)

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print documents instead of pushing to Elasticsearch",
    )
    parser.add_argument(
        "--interval",
        metavar="DURATION",
        required=True,
        help=(
            "Push interval (e.g. 5m, 1h). "
            "Stored as meta.interval so get-new-activity can compute uptime."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="Process only N servers",
    )
    parser.add_argument(
        "--metadata",
        action="append",
        metavar="KEY=VALUE",
        help=(
            "Additional metadata to add to documents as meta.KEY (can be specified "
            "multiple times); the report commands' --hub selects on hub=NAME"
        ),
    )
    return parser


def _validate_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Run post-parse argument validation."""
    validate_jupyterhub_arguments(args, parser)

    # --debug excuses a complete Elasticsearch group,
    # but not a flag that would be useless even without it.
    reject_es_flags_without_endpoint(args, parser)
    if not args.debug:
        if not args.es_endpoint:
            parser.error("--es-endpoint is required unless --debug is used")
        if not args.es_index:
            parser.error("--es-index is required unless --debug is used")
        validate_es_arguments(args, parser)

    positive_int_or_error(args.limit, parser=parser, flag="--limit")

    # The run checks --interval and --metadata again as it uses them;
    # checking them here reports a mistake as a usage error.
    interval = parse_duration(args.interval)
    if interval is None:
        parser.error(f"--interval: invalid duration {args.interval!r}")
    if interval.total_seconds() <= 0:
        parser.error(f"--interval must be positive, not {args.interval!r}")
    try:
        parse_metadata(args.metadata)
    except DataShapeError as e:
        parser.error(str(e))


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Raises:
        DataShapeError: If ``--metadata`` cannot be parsed.
        ExternalServiceError: If client creation or server push fails.
    """
    metadata = parse_metadata(args.metadata)
    jupyterhub_client = make_jupyterhub_client(args)

    if args.debug:
        print("Debug mode: documents will be printed, not pushed to Elasticsearch")
        successful, errors = push_servers(
            jupyterhub_client=jupyterhub_client,
            sink=PrintingSink(),
            interval=args.interval,
            limit=args.limit,
            metadata=metadata,
        )
    else:
        with make_es_client(args) as es_client:
            successful, errors = push_servers(
                jupyterhub_client=jupyterhub_client,
                sink=ElasticsearchSink(es_client, args.es_index),
                interval=args.interval,
                limit=args.limit,
                metadata=metadata,
            )

    print(f"\nProcessing complete: {successful} successful, {errors} errors")

    if errors > 0:
        return 1
    return 0


def main() -> int:
    """Main entry point for the push-servers script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
