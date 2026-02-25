"""Command-line tool for deleting Elasticsearch API keys."""

import argparse
import getpass
import sys
from pathlib import Path

from app.elasticsearch_client import ElasticsearchClient


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Invalidate an Elasticsearch API key using username and password",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --endpoint https://elastic.example.com:9200 --id abc123
  %(prog)s --endpoint https://elastic.example.com:9200 --name "my-api-key"
  %(prog)s --endpoint https://elastic.example.com:9200 --id abc123 --ca-cert /path/to/ca.crt
        """,
    )

    # Elasticsearch connection parameters
    parser.add_argument(
        "--endpoint",
        required=True,
        help="Elasticsearch API endpoint URL (e.g., https://elastic.example.com:9200)",
    )
    parser.add_argument(
        "--ca-cert",
        type=Path,
        help="Path to CA certificate file for TLS verification",
    )

    # Authentication parameters
    parser.add_argument(
        "--username",
        help="Username for authentication (will prompt if not provided)",
    )

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

    args = parser.parse_args()

    # Validate CA certificate exists if provided
    if args.ca_cert and not args.ca_cert.exists():
        parser.error(f"CA certificate file not found: {args.ca_cert}")

    return args


def _get_credentials(username_arg: str | None) -> tuple[str, str] | None:
    """Prompt for username and password interactively.

    Args:
        username_arg: Pre-supplied username, or None to prompt.

    Returns:
        A (username, password) tuple, or None if the user cancelled.
    """
    if username_arg:
        username = username_arg
    else:
        try:
            username = input("Username: ")
        except (EOFError, KeyboardInterrupt):
            print("\nOperation cancelled.", file=sys.stderr)
            return None

    try:
        password = getpass.getpass("Password: ")
    except (EOFError, KeyboardInterrupt):
        print("\nOperation cancelled.", file=sys.stderr)
        return None

    return username, password


def main() -> int:
    """Main entry point for the delete-es-api-key script.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        args = parse_arguments()

        # Get credentials interactively
        credentials = _get_credentials(args.username)
        if credentials is None:
            return 1
        username, password = credentials

        # Validate that username and password are not empty
        if not username or not password:
            print("Error: Username and password are required.", file=sys.stderr)
            return 1

        # Invalidate the API key
        try:
            result = ElasticsearchClient.delete_api_key_with_basic_auth(
                endpoint=args.endpoint,
                username=username,
                password=password,
                ca_cert=str(args.ca_cert) if args.ca_cert else None,
                key_id=args.key_id,
                key_name=args.key_name,
            )
        except ValueError as e:
            print(f"Error invalidating API key: {e}", file=sys.stderr)
            return 1

        invalidated = result.get("invalidated_api_keys", [])
        previously = result.get("previously_invalidated_api_keys", [])
        error_count = result.get("error_count", 0)

        if invalidated:
            print(
                f"Invalidated {len(invalidated)} API key(s): {', '.join(invalidated)}"
            )
        if previously:
            print(
                f"Already invalidated: {len(previously)} key(s): "
                f"{', '.join(previously)}"
            )
        if not invalidated and not previously:
            print("No matching API keys found.")
        if error_count:
            print(f"Errors: {error_count}", file=sys.stderr)
            return 1

        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
