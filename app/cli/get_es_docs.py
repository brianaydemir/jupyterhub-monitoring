"""Command-line tool for querying Elasticsearch documents."""

import argparse
import json
import sys

from app.cli.runtime import positive_int_or_error, run_command
from app.cli.utils import (
    configure_es_admin_parser,
    make_es_client,
    validate_es_admin_arguments,
)
from app.core.errors import ExternalServiceError


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser for this command."""
    parser = argparse.ArgumentParser(
        description=(
            "Query Elasticsearch and retrieve documents using " "a Kibana-style query string"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --es-endpoint https://es.example.com:9200 --es-api-key /path/to/api-key --es-index logs --query "status:200"
  %(prog)s --es-endpoint https://es.example.com:9200 --es-api-key /path/to/api-key --es-index logs --query "level:error AND timestamp:[now-1d TO now]"
  %(prog)s --es-endpoint https://es.example.com:9200 --es-api-key /path/to/api-key --es-index logs --query "*" --es-ca-cert /path/to/ca.crt

Environment variables:
  ELASTICSEARCH_API_KEY  Elasticsearch API key (used when --es-api-key is not provided)
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
            "Maximum number of documents to retrieve and display. "
            "Results are sorted in descending order by meta.snapshot-time."
        ),
    )
    return parser


def _validate_arguments(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Run all post-parse argument validation for this command."""
    validate_es_admin_arguments(args, parser)
    positive_int_or_error(args.limit, parser=parser, flag="--limit")


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Raises:
        app.errors.ExternalServiceError: If Elasticsearch query execution fails.
    """
    try:
        with make_es_client(args) as client:
            sort = (
                [{"meta.snapshot-time": {"order": "desc"}}] if args.limit is not None else None
            )
            for document in client.query(
                index=args.es_index,
                query_string=args.query,
                sort=sort,
                limit=args.limit,
            ):
                print(json.dumps(document, indent=2))
        return 0
    except Exception as e:
        raise ExternalServiceError(f"Querying Elasticsearch failed: {e}") from e


def main() -> int:
    """Main entry point for the get-es-docs script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
