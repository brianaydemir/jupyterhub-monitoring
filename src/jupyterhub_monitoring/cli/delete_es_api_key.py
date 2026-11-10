"""Command-line tool for deleting Elasticsearch API keys."""

import argparse
import sys
from typing import Any, cast

from jupyterhub_monitoring.cli.arguments import add_es_connection_argument_group
from jupyterhub_monitoring.cli.factories import make_es_client
from jupyterhub_monitoring.cli.runtime import run_command
from jupyterhub_monitoring.cli.validation import validate_es_arguments


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for this command."""
    parser = argparse.ArgumentParser(
        description="Invalidate an Elasticsearch API key owned by the authenticated user",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --es-endpoint https://elastic.example.com:9200 \\
    --es-username elastic --id abc123
  %(prog)s --es-endpoint https://elastic.example.com:9200 \\
    --es-username elastic --name "my-api-key"

Environment variables:
  ELASTICSEARCH_API_KEY  API key (when --es-api-key is
                         not provided)
        """,
    )

    add_es_connection_argument_group(parser)

    key_group = parser.add_mutually_exclusive_group(required=True)
    key_group.add_argument(
        "--id",
        dest="key_id",
        metavar="ID",
        help="ID of the API key to invalidate",
    )
    key_group.add_argument(
        "--name",
        dest="key_name",
        metavar="NAME",
        help="Name of the API key(s) to invalidate",
    )

    return parser


def _validate_arguments(
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> None:
    """Run post-parse argument validation."""
    validate_es_arguments(args, parser)


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Raises:
        ExternalServiceError: If key invalidation fails.
    """
    with make_es_client(args) as client:
        result = client.delete_api_key(
            key_id=args.key_id,
            key_name=args.key_name,
        )

    invalidated = result.get("invalidated_api_keys", [])
    previously = result.get("previously_invalidated_api_keys", [])
    error_count = result.get("error_count", 0)

    if invalidated:
        print(f"Invalidated {len(invalidated)} API key(s): {', '.join(invalidated)}")
    if previously:
        print(f"Already invalidated: {len(previously)} key(s): {', '.join(previously)}")
    if not invalidated and not previously and not error_count:
        print("No matching API keys found.")
    if error_count:
        print(f"Errors: {error_count}", file=sys.stderr)
        for detail in result.get("error_details", []):
            print(f"  {_error_detail(detail)}", file=sys.stderr)
        return 1

    return 0


def _error_detail(detail: object) -> str:
    """Summarize one entry of an ``error_details`` response array.

    Elasticsearch reports these as nested cause objects;
    the type and reason are what identifies the failure.
    """
    if not isinstance(detail, dict):
        return str(detail)
    typed = cast(dict[str, Any], detail)
    kind = typed.get("type", "error")
    reason = typed.get("reason", "no reason given")
    return f"{kind}: {reason}"


def main() -> int:
    """Main entry point for the delete-es-api-key script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
