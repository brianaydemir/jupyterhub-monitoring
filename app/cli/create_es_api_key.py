"""Command-line tool for obtaining Elasticsearch API keys."""

import argparse
import json
import sys
from typing import Any

from app.cli.runtime import run_command
from app.cli.utils import (
    configure_es_admin_parser,
    make_es_client,
    validate_es_admin_arguments,
)
from app.core.errors import ExternalServiceError


def format_output_key(result: dict[str, Any]) -> str:
    """Format an API-key response as key-only output."""
    # Get the encoded value, ensuring it's a string
    encoded = result.get("encoded", "")
    if not isinstance(encoded, str):
        return ""
    return encoded


def format_output_json(result: dict[str, Any]) -> str:
    """Format the API key result as JSON."""
    # Extract the essential fields for JSON output
    output_data = {
        "id": result.get("id"),
        "name": result.get("name"),
        "api_key": result.get("api_key"),
        "encoded": result.get("encoded"),
    }

    # Include expiration if present
    if "expiration" in result:
        output_data["expiration"] = result["expiration"]

    return json.dumps(output_data, indent=2)


def format_output_full(result: dict[str, Any]) -> str:
    """Format the API key result as full detailed output."""
    lines = ["Elasticsearch API Key Created Successfully:", ""]
    lines.append(f"ID:       {result.get('id', 'N/A')}")
    lines.append(f"Name:     {result.get('name', 'N/A')}")
    lines.append(f"API Key:  {result.get('api_key', 'N/A')}")
    lines.append(f"Encoded:  {result.get('encoded', 'N/A')}")

    if "expiration" in result:
        lines.append(f"Expires:  {result['expiration']}")
    else:
        lines.append("Expires:  Never")

    lines.append("")
    lines.append("Use the 'Encoded' value for API authentication.")

    return "\n".join(lines)


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser for this command."""
    parser = argparse.ArgumentParser(
        description="Obtain an Elasticsearch API key using username and password",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --es-endpoint https://elastic.example.com:9200
  %(prog)s --es-endpoint https://elastic.example.com:9200 --name "my-api-key" --expiration "7d"
  %(prog)s --es-endpoint https://elastic.example.com:9200 --format json
  %(prog)s --es-endpoint https://elastic.example.com:9200 --es-ca-cert /path/to/ca.crt

Expiration format:
  Specify expiration time using Elasticsearch time units:
  - "1d" = 1 day
  - "7d" = 7 days
  - "30d" = 30 days
  - "12h" = 12 hours
  - Omit --expiration for a key that never expires
        """,
    )

    configure_es_admin_parser(parser, include_index=False)

    # API key parameters
    parser.add_argument(
        "--name",
        help="Name for the API key (defaults to 'api-key-{username}')",
    )
    parser.add_argument(
        "--expiration",
        help=(
            "Expiration time for the API key (e.g., '1d', '7d', '30d'). "
            "If not specified, the key never expires."
        ),
    )

    # Output format
    parser.add_argument(
        "--format",
        choices=["key", "json", "full"],
        default="key",
        help=(
            "Output format: 'key' (default, prints only the API key), "
            "'json' (JSON output), 'full' (detailed human-readable output)"
        ),
    )

    return parser


def _validate_arguments(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Run all post-parse argument validation for this command."""
    validate_es_admin_arguments(args, parser)


def _run(args: argparse.Namespace) -> int:
    """Execute command business logic.

    Raises:
        ExternalServiceError: If Elasticsearch API-key creation fails.
    """
    try:
        with make_es_client(args) as client:
            result = client.create_api_key(
                key_name=args.name,
                expiration=args.expiration,
                username=args.es_username,
            )

        if args.format == "key":
            output = format_output_key(result)
        elif args.format == "json":
            output = format_output_json(result)
        else:  # full
            output = format_output_full(result)

        print(output)
        return 0
    except Exception as e:
        raise ExternalServiceError(f"Creating API key failed: {e}") from e


def main() -> int:
    """Main entry point for the create-es-api-key script."""
    return run_command(_build_parser, _run, validators=[_validate_arguments])


if __name__ == "__main__":
    sys.exit(main())
