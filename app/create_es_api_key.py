"""Command-line tool for obtaining Elasticsearch API keys."""

import argparse
import json
import sys
from typing import Any

from app.cli_utils import (
    add_elasticsearch_basic_argument_group,
    prompt_credentials,
    validate_elasticsearch_basic_arguments,
)
from app.elasticsearch_client import ElasticsearchClient


def format_output_key_only(result: dict[str, Any]) -> str:
    """Format the API key result as key-only output.

    Args:
        result: The API key creation result from Elasticsearch

    Returns:
        The encoded API key string (id:api_key format)
    """
    # Get the encoded value, ensuring it's a string
    encoded = result.get("encoded", "")
    if not isinstance(encoded, str):
        return ""
    return encoded


def format_output_json(result: dict[str, Any]) -> str:
    """Format the API key result as JSON.

    Args:
        result: The API key creation result from Elasticsearch

    Returns:
        JSON formatted string with id, name, api_key, and encoded fields
    """
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
    """Format the API key result as full detailed output.

    Args:
        result: The API key creation result from Elasticsearch

    Returns:
        Human-readable formatted string with all API key details
    """
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


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Obtain an Elasticsearch API key using username and password",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --elasticsearch-endpoint https://elastic.example.com:9200
  %(prog)s --elasticsearch-endpoint https://elastic.example.com:9200 --name "my-api-key" --expiration "7d"
  %(prog)s --elasticsearch-endpoint https://elastic.example.com:9200 --format json
  %(prog)s --elasticsearch-endpoint https://elastic.example.com:9200 --elasticsearch-ca-cert /path/to/ca.crt

Expiration format:
  Specify expiration time using Elasticsearch time units:
  - "1d" = 1 day
  - "7d" = 7 days
  - "30d" = 30 days
  - "12h" = 12 hours
  - Omit --expiration for a key that never expires
        """,
    )

    # Elasticsearch connection parameters
    add_elasticsearch_basic_argument_group(parser)

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

    # Authentication parameters
    parser.add_argument(
        "--username",
        help="Username for authentication (will prompt if not provided)",
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

    args = parser.parse_args()

    # Validate CA certificate exists if provided
    validate_elasticsearch_basic_arguments(args, parser)

    return args


def main() -> int:
    """Main entry point for the create-es-api-key script.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        args = parse_arguments()

        # Get credentials (username from CLI or prompt; password always prompted)
        credentials = prompt_credentials(args.username)
        if credentials is None:
            return 1
        username, password = credentials

        # Validate that username and password are not empty
        if not username or not password:
            print("Error: Username and password are required.", file=sys.stderr)
            return 1

        # Create the API key
        try:
            result = ElasticsearchClient.create_api_key_with_basic_auth(
                endpoint=args.elasticsearch_endpoint,
                username=username,
                password=password,
                ca_cert=(
                    str(args.elasticsearch_ca_cert)
                    if args.elasticsearch_ca_cert
                    else None
                ),
                key_name=args.name,
                expiration=args.expiration,
            )
        except ValueError as e:
            print(f"Error creating API key: {e}", file=sys.stderr)
            return 1

        # Format and print the output based on the selected format
        if args.format == "key":
            output = format_output_key_only(result)
        elif args.format == "json":
            output = format_output_json(result)
        else:  # full
            output = format_output_full(result)

        print(output)
        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
