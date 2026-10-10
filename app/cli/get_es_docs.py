"""Command-line tool for querying Elasticsearch documents."""

import argparse
import json
import sys

from app.cli.arguments import configure_es_admin_parser
from app.cli.factories import make_es_client
from app.cli.runtime import run_command
from app.cli.validation import positive_int_or_error, validate_es_admin_arguments


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for this command."""
    parser = argparse.ArgumentParser(
        description="Query Elasticsearch for documents using a Kibana-style query string",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --es-endpoint https://es.example.com:9200 \\
    --es-api-key /path/to/key --es-index logs \\
    --query "status:200"
  %(prog)s --es-endpoint https://es.example.com:9200 \\
    --es-api-key /path/to/key --es-index logs \\
    --query "level:error AND timestamp:[now-1d TO now]"

Environment variables:
  ELASTICSEARCH_API_KEY  API key (when --es-api-key is
                         not provided)
        """,
    )

    configure_es_admin_parser(parser, include_index=True)

    parser.add_argument(
        "--query",
        required=True,
        help='Kibana-style query string (e.g., "status:200", "field:value AND other:*")',
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help=(
            "Maximum number of documents to retrieve. "
            "Results are sorted descending by "
            "meta.snapshot-time."
        ),
    )
    return parser


def _validate_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Run post-parse argument validation."""
    validate_es_admin_arguments(args, parser)
    positive_int_or_error(args.limit, parser=parser, flag="--limit")


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Raises:
        ExternalServiceError: If query execution fails.
    """
    # unmapped_type keeps the sort from failing on an index that has no
    # meta.snapshot-time field at all.
    newest_first = {"meta.snapshot-time": {"order": "desc", "unmapped_type": "long"}}
    sort = [newest_first] if args.limit is not None else None
    with make_es_client(args) as client:
        for document in client.query(
            index=args.es_index,
            query_string=args.query,
            sort=sort,
            limit=args.limit,
        ):
            print(json.dumps(document, indent=2))

    return 0


def main() -> int:
    """Main entry point for the get-es-docs script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
