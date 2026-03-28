"""Command-line tool for deleting Elasticsearch API keys."""

import argparse
import sys

from app.cli.runtime import run_command
from app.cli.utils import (
    configure_es_admin_parser,
    make_es_client,
    validate_es_admin_arguments,
)
from app.core.errors import ExternalServiceError


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser for this command."""
    parser = argparse.ArgumentParser(
        description="Invalidate an Elasticsearch API key using username and password",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --es-endpoint https://elastic.example.com:9200 --id abc123
  %(prog)s --es-endpoint https://elastic.example.com:9200 --name "my-api-key"
  %(prog)s --es-endpoint https://elastic.example.com:9200 --id abc123 --es-ca-cert /path/to/ca.crt
        """,
    )

    configure_es_admin_parser(parser, include_index=False)

    # API key selection (exactly one required)
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


def _validate_arguments(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Run all post-parse argument validation for this command."""
    validate_es_admin_arguments(args, parser)


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Raises:
        app.errors.ExternalServiceError: If key invalidation fails.
    """
    try:
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
        if not invalidated and not previously:
            print("No matching API keys found.")
        if error_count:
            print(f"Errors: {error_count}", file=sys.stderr)
            return 1

        return 0
    except ValueError as e:
        raise ExternalServiceError(f"Invalidating API key failed: {e}") from e
    except Exception as e:
        raise ExternalServiceError(f"Invalidating API key failed: {e}") from e


def main() -> int:
    """Main entry point for the delete-es-api-key script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
