"""Command-line tool for listing Elasticsearch API keys."""

import argparse
import datetime
import json
import sys
from typing import Any

from app.cli_utils import (
    add_elasticsearch_basic_argument_group,
    prompt_credentials,
    validate_elasticsearch_basic_arguments,
)
from app.elasticsearch_client import ElasticsearchClient
from app.time_utils import get_now_ms


def _ms_to_datetime(ms: int | None) -> str:
    """Convert a millisecond epoch timestamp to a human-readable UTC string."""
    if ms is None:
        return "Never"
    return datetime.datetime.fromtimestamp(
        ms / 1000, tz=datetime.timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")


def _key_status_tags(key: dict[str, Any]) -> list[str]:
    """Return status tags for a key that is not fully active."""
    tags = []
    if key.get("invalidated"):
        tags.append("invalidated")
    expiration = key.get("expiration")
    if expiration is not None:
        now_ms = get_now_ms()
        if expiration <= now_ms:
            tags.append("expired")
    return tags


def format_output_key(keys: list[dict[str, Any]], *, active_only: bool = True) -> str:
    """Format API keys as a simple id/name listing.

    Args:
        keys: List of API key dictionaries from Elasticsearch
        active_only: Whether the list was filtered to active keys only

    Returns:
        One line per key in the form "id  name" with optional status tags
    """
    if not keys:
        return "(no active API keys)" if active_only else "(no API keys)"
    lines = []
    for key in keys:
        line = f"{key.get('id', '')}  {key.get('name', '')}"
        tags = _key_status_tags(key)
        if tags:
            line += f"  [{', '.join(tags)}]"
        lines.append(line)
    return "\n".join(lines)


def format_output_json(keys: list[dict[str, Any]]) -> str:
    """Format API keys as a pretty-printed JSON array.

    Args:
        keys: List of API key dictionaries from Elasticsearch

    Returns:
        Pretty-printed JSON string
    """
    return json.dumps(keys, indent=2)


def format_output_full(keys: list[dict[str, Any]], *, active_only: bool = True) -> str:
    """Format API keys as a human-readable table.

    Args:
        keys: List of API key dictionaries from Elasticsearch
        active_only: Whether the list was filtered to active keys only

    Returns:
        Human-readable formatted string with id, name, creation, and expiration
    """
    label = "Active API Keys" if active_only else "API Keys"
    if not keys:
        return f"No {label.lower()} found."

    lines = [f"{label} ({len(keys)}):", ""]
    for key in keys:
        lines.append(f"  ID:          {key.get('id', 'N/A')}")
        lines.append(f"  Name:        {key.get('name', 'N/A')}")
        lines.append(f"  Created:     {_ms_to_datetime(key.get('creation'))}")
        expiration = key.get("expiration")
        expiration_str = _ms_to_datetime(expiration)
        if expiration is not None:
            now_ms = get_now_ms()
            if expiration <= now_ms:
                expiration_str += "  (expired)"
        lines.append(f"  Expires:     {expiration_str}")
        if not active_only:
            invalidated = key.get("invalidated", False)
            lines.append(f"  Invalidated: {invalidated}")
        lines.append("")
    return "\n".join(lines).rstrip()


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description=("List Elasticsearch API keys owned by the authenticated user"),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --elasticsearch-endpoint https://elastic.example.com:9200
  %(prog)s --elasticsearch-endpoint https://elastic.example.com:9200 --all
  %(prog)s --elasticsearch-endpoint https://elastic.example.com:9200 --format json
  %(prog)s --elasticsearch-endpoint https://elastic.example.com:9200 --format full
  %(prog)s --elasticsearch-endpoint https://elastic.example.com:9200 --elasticsearch-ca-cert /path/to/ca.crt
        """,
    )

    # Elasticsearch connection parameters
    add_elasticsearch_basic_argument_group(parser)

    # Authentication parameters
    parser.add_argument(
        "--username",
        help="Username for authentication (will prompt if not provided)",
    )

    # Filter options
    parser.add_argument(
        "--all",
        dest="all_keys",
        action="store_true",
        help="Include expired and invalidated keys (default: active keys only)",
    )

    # Output format
    parser.add_argument(
        "--format",
        choices=["key", "json", "full"],
        default="key",
        help=(
            "Output format: 'key' (default, prints id and name per line), "
            "'json' (JSON array), 'full' (detailed human-readable output)"
        ),
    )

    args = parser.parse_args()

    # Validate CA certificate exists if provided
    validate_elasticsearch_basic_arguments(args, parser)

    return args


def main() -> int:
    """Main entry point for the list-es-api-keys script.

    Returns:
        Exit code (0 for success, non-zero for error)
    """
    try:
        args = parse_arguments()

        credentials = prompt_credentials(args.username)
        if credentials is None:
            return 1
        username, password = credentials

        try:
            keys = ElasticsearchClient.list_api_keys_with_basic_auth(
                endpoint=args.elasticsearch_endpoint,
                username=username,
                password=password,
                ca_cert=(
                    str(args.elasticsearch_ca_cert)
                    if args.elasticsearch_ca_cert
                    else None
                ),
                active_only=not args.all_keys,
            )
        except ValueError as e:
            print(f"Error listing API keys: {e}", file=sys.stderr)
            return 1

        active_only = not args.all_keys
        if args.format == "key":
            output = format_output_key(keys, active_only=active_only)
        elif args.format == "json":
            output = format_output_json(keys)
        else:  # full
            output = format_output_full(keys, active_only=active_only)

        print(output)
        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
